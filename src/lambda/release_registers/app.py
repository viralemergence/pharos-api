import os
from time import time

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field, ValidationError
from register import Dataset, DatasetReleaseStatus, Register, ReleaseReport

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

    METADATA_TABLE.update_item(
        Key={"pk": validated.project_id, "sk": validated.dataset_id},
        UpdateExpression="set releaseStatus = :r",
        ExpressionAttributeValues={":r": DatasetReleaseStatus.RELEASING.value},
    )

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

        dataset_response = METADATA_TABLE.get_item(
            Key={"pk": validated.project_id, "sk": validated.dataset_id},
        )
        dataset = Dataset.parse_table_item(dataset_response.get("Item", None))

        dataset.release_report = release_report
        dataset.release_status = release_report.release_status

        METADATA_TABLE.put_item(Item=dataset.table_item())

    except ValidationError or ClientError:
        METADATA_TABLE.update_item(
            Key={"pk": validated.project_id, "sk": validated.dataset_id},
            UpdateExpression="set releaseStatus = :r",
            ExpressionAttributeValues={":r": DatasetReleaseStatus.UNRELEASED.value},
        )
        raise
