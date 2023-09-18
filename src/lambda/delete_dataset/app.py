import os

import boto3
from botocore.client import ClientError

from pydantic import BaseModel, Extra, Field, ValidationError
from auth import check_auth
from format import format_response
from register import Dataset

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class DeleteDatasetBody(BaseModel):
    """Event data payload to upload a dataset."""

    researcher_id: str = Field(..., alias="researcherID")
    dataset: Dataset

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):
    try:
        validated = DeleteDatasetBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    user = check_auth(validated.researcher_id)

    # check if the user is valid and has access to the project
    if (
        not user
        or not user.project_ids
        or not validated.dataset.project_id in user.project_ids
    ):
        return format_response(403, "Not Authorized")

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
