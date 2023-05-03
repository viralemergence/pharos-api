"""Lambda function to check if a stored register is valid and ready to release."""
import os

import boto3
from botocore.exceptions import ClientError

from pydantic import BaseModel, Field
from pydantic.error_wrappers import ValidationError

from auth import check_auth
from format import format_response
from register import DatasetReleaseStatus, Register

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class ReleaseDatasetBody(BaseModel):
    """Event data payload to release a dataset."""

    researcher_id: str = Field(..., alias="researcherID")
    project_id: str = Field(..., alias="projectID")
    dataset_id: str = Field(..., alias="datasetID")


def lambda_handler(event, _):
    try:
        validated = ReleaseDatasetBody.parse_raw(event["body"])
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    user = check_auth(validated.researcher_id)
    if not user:
        return format_response(403, "Not Authorized")
    if not user.project_ids or not validated.project_id in user.project_ids:
        return format_response(403, "Researcher is not authorized for this project")

    try:
        key_list = S3CLIENT.list_objects_v2(
            Bucket=DATASETS_S3_BUCKET, Prefix=f"{validated.dataset_id}/"
        )["Contents"]

        key_list.sort(key=lambda item: item["LastModified"], reverse=True)
        key = key_list[0]["Key"]

        register_response = S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)
        register_json = register_response["Body"].read().decode("UTF-8")

    except (ValueError, ClientError):
        return format_response(400, "Dataset not found")

    try:
        register = Register.parse_raw(register_json)
        release_report = register.get_release_report()

        # need to actually set released value in the dataset metadata object in dynamodb
        if release_report.release_status == DatasetReleaseStatus.RELEASED:
            METADATA_TABLE.update_item(
                Key={"pk": validated.project_id, "sk": validated.dataset_id},
                UpdateExpression="set releaseStatus = :r",
                ExpressionAttributeValues={":r": DatasetReleaseStatus.RELEASED.value},
            )

        return format_response(
            200, release_report.json(by_alias=True), preformatted=True
        )

    except ValidationError as e:
        return {
            "statusCode": 400,
            "body": f'{{"message":"Couldn\'t release dataset.", "error":"{e.json()}}}',
        }
