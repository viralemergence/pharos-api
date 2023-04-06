"""Lambda function to check if a stored register is valid and ready to release."""
import os

import boto3
from botocore.exceptions import ClientError

from pydantic import BaseModel
from pydantic.error_wrappers import ValidationError

from auth import check_auth
from format import format_response
from register import DatasetReleaseStatus, Register

DYNAMODB = boto3.resource("dynamodb")
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class ReleaseDatasetBody(BaseModel):
    """Event data payload to release a dataset."""

    datasetID: str
    researcherID: str


def lambda_handler(event, _):
    try:
        validated = ReleaseDatasetBody.parse_raw(event["body"])
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    authorized = check_auth(validated.researcherID)
    if not authorized:
        return format_response(403, "Not Authorized")

    try:
        key_list = S3CLIENT.list_objects_v2(
            Bucket=DATASETS_S3_BUCKET, Prefix=f"{validated.datasetID}/"
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
        if release_report.releaseStatus == DatasetReleaseStatus.RELEASED:
            DATASETS_TABLE.update_item(
                Key={"datasetID": validated.datasetID, "recordID": "_meta"},
                UpdateExpression="set releaseStatus = :r",
                ExpressionAttributeValues={":r": DatasetReleaseStatus.RELEASED.value},
            )

        return format_response(200, release_report.json(), preformatted=True)

    except ValidationError as e:
        return {
            "statusCode": 400,
            "body": f'{{"message":"Couldn\'t release dataset.", "error":"{e.json()}}}',
        }
