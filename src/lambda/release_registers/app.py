import os
from time import time

import boto3
from botocore.exceptions import ClientError
from format import format_response
from pydantic import BaseModel, Field, ValidationError
from register import DatasetReleaseStatus, Register, ReleaseReport

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class ReleaseRegistersData(BaseModel):
    """Event data payload to validate a dataset."""

    project_id: str = Field(alias="projectID")
    dataset_id: str = Field(alias="datasetID")


def lambda_handler(event, _):
    validated = ReleaseRegistersData.parse_obj(event)

    try:
        item_list = S3CLIENT.list_objects_v2(
            Bucket=DATASETS_S3_BUCKET, Prefix=f"{validated.dataset_id}/"
        )["Contents"]

        release_report = ReleaseReport()

        for item in item_list:
            start = time()
            key = item["Key"]

            register_response = S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)
            register_json = register_response["Body"].read().decode("UTF-8")
            register = Register.parse_raw(register_json)

            release_report = ReleaseReport.merge(
                release_report, register.get_release_report()
            )

            print(f"Validate {key}: {time() - start}")

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

    except ValidationError or ClientError as e:
        return {
            "statusCode": 400,
            "body": f'{{"message":"Couldn\'t release dataset.", "error":"{e.json()}}}',
        }
