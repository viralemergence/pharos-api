import os
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from engine import get_engine
from models import Base, PublishedDataset, PublishedProject, PublishedRecord
from publish_register import (
    create_published_dataset,
    create_published_project,
    create_published_records,
    upsert_project_users,
)
from pydantic import BaseModel, Extra
from register import Dataset, DatasetReleaseStatus, Project, ProjectPublishStatus, User
from sqlalchemy import select
from sqlalchemy.orm import Session

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]

CF_CLIENT = boto3.client("cloudfront")
CF_CACHE_POLICY_ID = os.environ["CF_CACHE_POLICY_ID"]


class PublishRegistersData(BaseModel):
    """Event data payload to publish the registers of the project."""

    project: Project
    released_datasets: list[Dataset]
    project_users: list[User]

    class Config:
        extra = Extra.forbid


def download_and_create_published_records(
    published_dataset: PublishedDataset, dataset: Dataset, key: str
) -> list[PublishedRecord]:

    start = time.time()
    try:
        register_json = (
            S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)["Body"]
            .read()
            .decode("utf-8")
        )

        start = time.time()

        records = create_published_records(
            register_json=register_json,
            project_id=published_dataset.project_id,
            dataset_id=dataset.dataset_id,
        )

        print("Load register and create records", time.time() - start)

        return records

    except ClientError as e:
        dataset.release_status = DatasetReleaseStatus.UNRELEASED
        dataset.last_updated = datetime.utcnow().isoformat() + "Z"
        METADATA_TABLE.put_item(Item=dataset.table_item())
        raise e


def lambda_handler(event: dict, _):
    metadata = PublishRegistersData.parse_obj(event)

    start = time.time()
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("Get engine", time.time() - start)

    try:
        with Session(engine) as session:

            published_project = create_published_project(project=metadata.project)

            upsert_project_users(
                session=session,
                published_project=published_project,
                users=metadata.project_users,
            )

            session.add(published_project)
            session.commit()

        # This loop could be moved to multiple parallel lambdas
        for dataset in metadata.released_datasets:
            print("Publishing Dataset", dataset.dataset_id)
            start = time.time()

            with Session(engine) as session:
                published_project = session.scalar(
                    select(PublishedProject).where(
                        PublishedProject.project_id == metadata.project.project_id
                    )
                )

                if not published_project:
                    raise Exception("Project not found")

                published_dataset = create_published_dataset(dataset=dataset)
                published_project.datasets.append(published_dataset)

                try:
                    item_list = S3CLIENT.list_objects_v2(
                        Bucket=DATASETS_S3_BUCKET, Prefix=f"{dataset.dataset_id}/"
                    )["Contents"]

                except ClientError as e:
                    dataset.release_status = DatasetReleaseStatus.UNRELEASED
                    dataset.last_updated = datetime.utcnow().isoformat() + "Z"
                    METADATA_TABLE.put_item(Item=dataset.table_item())
                    raise e

                for item in item_list:
                    published_dataset.records.extend(
                        download_and_create_published_records(
                            published_dataset=published_dataset,
                            dataset=dataset,
                            key=item["Key"],  # type: ignore
                        )
                    )

                    session.commit()

                print(f"Published dataset {dataset.name}", time.time() - start)

            dataset.release_status = DatasetReleaseStatus.PUBLISHED
            dataset.last_updated = datetime.utcnow().isoformat() + "Z"
            METADATA_TABLE.put_item(Item=dataset.table_item())

        print("Add dataset to project", time.time() - start)
        metadata.project.last_updated = datetime.utcnow().isoformat() + "Z"
        metadata.project.publish_status = ProjectPublishStatus.PUBLISHED
        METADATA_TABLE.put_item(Item=metadata.project.table_item())

        distributions = CF_CLIENT.list_distributions_by_cache_policy_id(
            CachePolicyId=CF_CACHE_POLICY_ID
        )

        cf_id = distributions.get("DistributionIdList", {}).get("Items")[0]

        if cf_id:
            invalidation = CF_CLIENT.create_invalidation(
                DistributionId=cf_id,
                InvalidationBatch={
                    "Paths": {"Quantity": 1, "Items": ["/*"]},
                    "CallerReference": f"{metadata.project.project_id}"
                    f"_{metadata.project.last_updated}",
                },
            )

            print(invalidation)

    except Exception as e:  # pylint: disable=broad-except
        print(e)
        metadata.project.last_updated = datetime.utcnow().isoformat() + "Z"
        metadata.project.publish_status = ProjectPublishStatus.UNPUBLISHED
        METADATA_TABLE.put_item(Item=metadata.project.table_item())
        return False

    return True
