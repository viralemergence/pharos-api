"""Lambda function to check if a stored register is valid and ready to release."""
import os

import boto3
from auth import check_auth
from botocore.exceptions import ClientError
from format import format_response
from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError
from register import Dataset, DatasetReleaseStatus

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]

LAMBDACLIENT = boto3.client("lambda")
RELEASE_REGISTERS_LAMBDA = os.environ["RELEASE_REGISTERS_LAMBDA"]


class ReleaseDatasetBody(BaseModel):
    """Event data payload to release a dataset."""

    project_id: str = Field(alias="projectID")
    dataset_id: str = Field(alias="datasetID")


def lambda_handler(event, _):
    try:
        user = check_auth(event)
    except ValidationError:
        return format_response(403, "Not Authorized")

    if not user:
        return format_response(403, "Not Authorized")
    if not user.project_ids:
        return format_response(404, "Researcher has no projects")

    try:
        validated = ReleaseDatasetBody.parse_raw(event["body"])
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    if not validated.project_id in user.project_ids:
        return format_response(403, "Researcher is not authorized for this project")

    try:
        dataset_response = METADATA_TABLE.get_item(
            Key={"pk": validated.project_id, "sk": validated.dataset_id},
        )

    except ClientError as e:
        return format_response(
            500,
            {
                "statusCode": 400,
                "body": f'{{"message":"Couldn\'t release dataset.", "error":{e}}}',
            },
            preformatted=True,
        )

    try:
        dataset = Dataset.parse_table_item(dataset_response.get("Item", None))

    except ValidationError as e:
        return format_response(
            500,
            {"body": f'{{"message":"Couldn\'t release dataset.", "error":{e.json()}}}'},
            preformatted=True,
        )

    if dataset.release_status != DatasetReleaseStatus.UNRELEASED:
        return format_response(
            500,
            {
                "message": "Couldn't release dataset.",
                "error": f"Dataset is in an unreleaseable state ({dataset.release_status})",
            },
        )

    try:
        LAMBDACLIENT.invoke(
            FunctionName=RELEASE_REGISTERS_LAMBDA,
            InvocationType="Event",
            Payload=ReleaseDatasetBody(
                projectID=dataset.project_id, datasetID=dataset.dataset_id
            ).json(by_alias=True),
        )

        return format_response(
            200, {"message": "Dataset is being validated for release."}
        )

    except ClientError as e:
        return format_response(
            500,
            {
                "statusCode": 400,
                "body": f'{{"message":"Couldn\'t release dataset.", "error":{e}}}',
            },
            preformatted=True,
        )
