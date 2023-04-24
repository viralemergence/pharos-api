from datetime import datetime
from typing import Union
import os
import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import ClientError
from pydantic import BaseModel, Extra, Field, ValidationError
from sqlalchemy.orm import Session

from auth import check_auth
from engine import get_engine
from format import format_response
from models import PublishedRecord
from register import Dataset, DatasetReleaseStatus, Project, ProjectPublishStatus


DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class UnpublishProjectData(BaseModel):
    researcher_id: str = Field(..., alias="researcherID")
    project_id: str = Field(..., alias="projectID")

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):
    try:
        validated = UnpublishProjectData.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    user = check_auth(validated.researcher_id)
    if not user or not user.project_ids or not validated.project_id in user.project_ids:
        return format_response(403, "Not Authorized")

    # Delete records from the database
    engine = get_engine()

    with Session(engine) as session:

        session.query(PublishedRecord).where(
            PublishedRecord.project_id == validated.project_id
        ).delete()

        session.commit()

    try:
        # Retrieve project metadata and datasets
        metadata_response = METADATA_TABLE.query(
            KeyConditionExpression=Key("pk").eq(validated.project_id)
        )

    except ClientError as e:
        print(e)
        return format_response(403, "Error retrieving project metadata")

    if not metadata_response["Items"]:
        return format_response(403, "Metadata not found")

    project: Union[Project, None] = None
    published_datasets: list[Dataset] = []
    for item in metadata_response["Items"]:
        if item["sk"] == "_meta":
            project = Project.parse_table_item(item)
            continue

        dataset = Dataset.parse_table_item(item)
        if dataset.release_status in [
            DatasetReleaseStatus.PUBLISHED,
            # letting this reset datasets stuck in "publishing" state for now
            DatasetReleaseStatus.PUBLISHING,
        ]:
            published_datasets.append(dataset)

    if not project:
        return format_response(403, "Project not found")

    # if len(published_datasets) == 0:
    #     return format_response(403, "No published datasets found")

    try:
        with METADATA_TABLE.batch_writer() as batch:
            # Update project metadata
            project.last_updated = datetime.utcnow().isoformat() + "Z"
            project.publish_status = ProjectPublishStatus.UNPUBLISHED
            batch.put_item(Item=project.table_item())

            # Update dataset metadata
            for dataset in published_datasets:
                # published datasets should go to released status
                dataset.last_updated = datetime.utcnow().isoformat() + "Z"
                dataset.release_status = DatasetReleaseStatus.RELEASED
                batch.put_item(Item=dataset.table_item())

    except ClientError as e:
        print(e)
        return format_response(403, "Error saving project and dataset metadata")

    return format_response(200, "Project records unpublished")
