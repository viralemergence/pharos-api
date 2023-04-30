from datetime import datetime
import os
import time

import boto3
from pydantic import BaseModel, Extra
from sqlalchemy.orm import Session

from engine import get_engine
from models import Base, PublishedDataset, PublishedRecord

from register import Dataset, DatasetReleaseStatus, Project, ProjectPublishStatus, User

from publish_register import (
    upsert_project_users,
    create_published_project,
    create_published_dataset,
    create_published_records,
)


SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class PublishRegistersData(BaseModel):
    """Event data payload to publish the registers of the project."""

    project: Project
    released_datasets: list[Dataset]
    project_users: list[User]

    class Config:
        extra = Extra.forbid


def download_and_create_published_records(
    published_dataset: PublishedDataset, dataset: Dataset
) -> list[PublishedRecord]:

    start = time.time()
    key = f"{dataset.dataset_id}/data.json"
    register_json = (
        S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)["Body"]
        .read()
        .decode("utf-8")
    )

    print("Load register", time.time() - start)
    start = time.time()

    records = create_published_records(
        register_json=register_json,
        project_id=published_dataset.project_id,
        dataset_id=dataset.dataset_id,
    )

    dataset.release_status = DatasetReleaseStatus.PUBLISHED
    dataset.last_updated = datetime.utcnow().isoformat() + "Z"
    METADATA_TABLE.put_item(Item=dataset.table_item())

    print("Add dataset to project", time.time() - start)

    return records


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

            # This loop could be moved to multiple parallel lambdas
            for dataset in metadata.released_datasets:
                published_dataset = create_published_dataset(dataset=dataset)

                published_dataset.records = download_and_create_published_records(
                    published_dataset=published_dataset,
                    dataset=dataset,
                )

                published_project.datasets.append(published_dataset)

            start = time.time()

            session.add(published_project)
            session.commit()

            print("Commit session", time.time() - start)

            metadata.project.last_updated = datetime.utcnow().isoformat() + "Z"
            metadata.project.publish_status = ProjectPublishStatus.PUBLISHED
            METADATA_TABLE.put_item(Item=metadata.project.table_item())

    except Exception as e:  # pylint: disable=broad-except
        print(e)
        metadata.project.last_updated = datetime.utcnow().isoformat() + "Z"
        metadata.project.publish_status = ProjectPublishStatus.UNPUBLISHED
        METADATA_TABLE.put_item(Item=metadata.project.table_item())
        return False

    return True
