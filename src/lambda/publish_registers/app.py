from datetime import datetime
import os
import time

import boto3
from pydantic import BaseModel, Extra
from sqlalchemy.orm import Session

from engine import get_engine
from models import Base, PublishedDataset, PublishedProject, Researcher
from publish_register import create_published_records
from register import Dataset, DatasetReleaseStatus, Project, ProjectPublishStatus, User

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


def upsert_project_users(
    session: Session, published_project: PublishedProject, users: list[User]
) -> None:

    start = time.time()

    researcher_ids = [user.researcher_id for user in users]

    # Get existing researchers from database
    existing_researchers = (
        session.query(Researcher)
        .filter(Researcher.researcher_id.in_(researcher_ids))
        .all()
    )
    print("Query existing researchers", time.time() - start)

    start = time.time()
    existing_researcher_ids = [r.researcher_id for r in existing_researchers]

    new_researchers: list[Researcher] = []

    # Create new researchers
    for user in users:
        if user.researcher_id in existing_researcher_ids:
            continue

        new_researchers.append(
            Researcher(
                researcher_id=user.researcher_id,
                name=user.name,
                organization=user.organization,
                email=user.email,
            )
        )

    published_project.researchers = existing_researchers + new_researchers

    print("Add new researchers", time.time() - start)


def add_datasets_to_project(
    published_project: PublishedProject, dataset_metadata: Dataset
) -> None:
    start = time.time()

    published_dataset = PublishedDataset()
    published_dataset.dataset_id = dataset_metadata.dataset_id
    published_dataset.project_id = dataset_metadata.project_id
    published_dataset.name = dataset_metadata.name

    key = f"{dataset_metadata.dataset_id}/data.json"
    register_json = (
        S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)["Body"]
        .read()
        .decode("utf-8")
    )
    print("Load register", time.time() - start)

    start = time.time()
    published_dataset.records = create_published_records(
        register_json=register_json,
        project_id=published_project.project_id,
        dataset_id=dataset_metadata.dataset_id,
    )

    dataset_metadata.release_status = DatasetReleaseStatus.PUBLISHED
    dataset_metadata.last_updated = datetime.utcnow().isoformat() + "Z"
    METADATA_TABLE.put_item(Item=dataset_metadata.table_item())

    published_project.datasets.append(published_dataset)

    print("Add dataset to project", time.time() - start)


def lambda_handler(event: dict, _):
    metadata = PublishRegistersData.parse_obj(event)

    start = time.time()
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("Get engine", time.time() - start)

    try:
        with Session(engine) as session:

            published_project = PublishedProject()
            published_project.project_id = metadata.project.project_id
            published_project.name = metadata.project.name
            published_project.description = metadata.project.description
            published_project.published_date = datetime.utcnow().date()

            upsert_project_users(
                session=session,
                published_project=published_project,
                users=metadata.project_users,
            )

            # This loop could be moved to multiple parallel lambdas
            for dataset_metadata in metadata.released_datasets:
                add_datasets_to_project(
                    published_project=published_project,
                    dataset_metadata=dataset_metadata,
                )

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
