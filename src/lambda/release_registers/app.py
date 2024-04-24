import os
from datetime import datetime
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

    dataset_response = METADATA_TABLE.get_item(
        Key={"pk": validated.project_id, "sk": validated.dataset_id},
    )
    dataset = Dataset.parse_table_item(dataset_response.get("Item", None))

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
        # start the release report as "released" so that the merge
        # of a successful report with the blank report will return
        # released status.
        release_report.release_status = DatasetReleaseStatus.RELEASED

        for item in item_list:
            start = time()
            key = item["Key"]  # type: ignore

            register_response = S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)
            register_json = register_response["Body"].read().decode("UTF-8")
            register = Register.parse_raw(register_json)

            release_report = ReleaseReport.merge(
                release_report, register.get_release_report()
            )

            print(f"Validate {key}: {time() - start}")

        # re-load the dataset to make sure this report will still be valid
        post_validation_dataset_response = METADATA_TABLE.get_item(
            Key={"pk": validated.project_id, "sk": validated.dataset_id},
        )
        post_validation_dataset = Dataset.parse_table_item(
            post_validation_dataset_response.get("Item", None)
        )

        # Only add the release report if there are no changes since the validation started
        if dataset.last_updated == post_validation_dataset.last_updated:
            post_validation_dataset.release_report = release_report
            post_validation_dataset.release_status = release_report.release_status
            post_validation_dataset.last_updated = datetime.utcnow().isoformat() + "Z"

            METADATA_TABLE.put_item(Item=post_validation_dataset.table_item())

    except (ValidationError, ClientError):
        METADATA_TABLE.update_item(
            Key={"pk": validated.project_id, "sk": validated.dataset_id},
            UpdateExpression="set releaseStatus = :r",
            ExpressionAttributeValues={":r": DatasetReleaseStatus.UNRELEASED.value},
        )
        raise
