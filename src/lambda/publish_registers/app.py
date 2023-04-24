from datetime import datetime
import os
import time

import boto3
from pydantic import BaseModel, Extra
from sqlalchemy.orm import Session

from engine import get_engine
from models import Researcher
from publish_register import publish_register_to_session
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


def lambda_handler(event: dict, _):
    metadata = PublishRegistersData.parse_obj(event)

    start = time.time()
    engine = get_engine()
    # Base.metadata.create_all(engine)
    print("Get engine", time.time() - start)

    try:
        with Session(engine) as session:
            researcher_ids = [user.researcher_id for user in metadata.project_users]

            start = time.time()
            # Get existing researchers
            existing_researchers = (
                session.query(Researcher)
                .filter(Researcher.researcher_id.in_(researcher_ids))
                .all()
            )
            print("Query existing researchers", time.time() - start)

            start = time.time()
            existing_researcher_ids = [r.researcher_id for r in existing_researchers]

            new_researchers: list[Researcher] = []

            # Add new researchers
            for user in metadata.project_users:
                if user.researcher_id in existing_researcher_ids:
                    continue

                new_researchers.append(
                    Researcher(researcher_id=user.researcher_id, name=user.name)
                )

            session.add_all(new_researchers)

            print("Add new researchers", time.time() - start)

            # This loop needs to be moved to multiple parallel lambdas
            for dataset in metadata.released_datasets:
                start = time.time()

                key = f"{dataset.dataset_id}/data.json"
                register_json = (
                    S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)["Body"]
                    .read()
                    .decode("utf-8")
                )
                print("Load register", time.time() - start)

                start = time.time()
                publish_register_to_session(
                    session=session,
                    register_json=register_json,
                    project_id=metadata.project.project_id,
                    dataset_id=dataset.dataset_id,
                    researchers=existing_researchers + new_researchers,
                )
                dataset.release_status = DatasetReleaseStatus.PUBLISHED
                dataset.last_updated = datetime.utcnow().isoformat() + "Z"
                METADATA_TABLE.put_item(Item=dataset.table_item())

                print("Publish register to session", time.time() - start)

            start = time.time()
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
