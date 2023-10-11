import os

import boto3
from botocore.client import ClientError

from pydantic import BaseModel, Extra, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session
from auth import check_auth
from engine import get_engine
from format import format_response
from models import PublishedDataset
from register import Dataset, DatasetReleaseStatus

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class DeleteDatasetBody(BaseModel):
    """Event data payload to upload a dataset."""

    dataset: Dataset

    class Config:
        extra = Extra.forbid


def delete_published_dataset(dataset: Dataset):
    engine = get_engine()

    with Session(engine) as session:
        published_dataset = session.scalar(
            select(PublishedDataset).where(
                PublishedDataset.dataset_id == dataset.dataset_id
            )
        )

        if not published_dataset:
            raise ValueError("Dataset not found in database")

        session.delete(published_dataset)
        session.commit()


def lambda_handler(event, _):

    try:
        user = check_auth(event)
    except ValidationError:
        return format_response(403, "Not Authorized")

    if not user:
        return format_response(403, "Not Authorized")

    try:
        validated = DeleteDatasetBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    # check if the user is valid and has access to the project
    if not user.project_ids or not validated.dataset.project_id in user.project_ids:
        return format_response(403, "Not Authorized")

    if validated.dataset.release_status == DatasetReleaseStatus.PUBLISHING:
        return format_response(
            403, "Cannot delete dataset while it is being published."
        )

    try:
        if validated.dataset.release_status == DatasetReleaseStatus.PUBLISHED:
            delete_published_dataset(validated.dataset)

    except ValueError as e:
        print(e)
        return format_response(403, "Error unpublishing dataset")

    try:

        METADATA_TABLE.delete_item(
            Key={"pk": validated.dataset.project_id, "sk": validated.dataset.dataset_id}
        )

        S3CLIENT.delete_object(
            Bucket=DATASETS_S3_BUCKET,
            Key=f"{validated.dataset.dataset_id}/data.json",
        )

        return format_response(200, "Dataset deleted.")

    except ClientError as e:
        print(e)
        return format_response(403, "Error deleting dataset")
