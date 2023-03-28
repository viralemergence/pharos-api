"""Lambda function to check if a stored register is valid and ready to release."""
import json
import os
from dataclasses import dataclass
from typing import Optional

import boto3
from pydantic import BaseModel
from pydantic.error_wrappers import ValidationError

from auth import check_auth
from format import format_response
from register import Register, ReportScore

DYNAMODB = boto3.resource("dynamodb")
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


@dataclass
class ReleaseReport:
    released: bool = False
    successCount: int = 0
    warningCount: int = 0
    failCount: int = 0
    missingCount: int = 0
    warningFields: dict[str, list] = {}
    failFields: dict[str, list] = {}
    missingFields: dict[str, list] = {}


REQUIRED_FIELDS = {
    "host_species",
    "latitude",
    "longitude",
    "collection_day",
    "collection_month",
    "collection_year",
    "detction_outcome",
    "pathogen",
}


class ReleaseDatasetBody(BaseModel):
    """Event data payload to release a dataset."""

    datasetID: str
    researcherID: str


def lambda_handler(event, _):
    try:
        event_body = ReleaseDatasetBody.parse_raw(event["body"])
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    authorized = check_auth(event_body.researcherID)
    if not authorized:
        return format_response(403, "Not Authorized")
    try:
        key_list = S3CLIENT.list_objects_v2(
            Bucket=DATASETS_S3_BUCKET, Prefix=f"{event_body.datasetID}/"
        )["Contents"]

        key_list.sort(key=lambda item: item["LastModified"], reverse=True)
        key = key_list[0]["Key"]

        register_response = S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)
        register_json = json.loads(register_response["Body"].read().decode("UTF-8"))
        register = Register.parse_raw(register_json)

        release_report = ReleaseReport()

        for recordID, record in register.register_data.items():
            for field in REQUIRED_FIELDS:
                if field not in record.__dict__:
                    release_report.missingCount += 1
                    if recordID not in release_report.missingFields:
                        release_report.missingFields[recordID] = []
                    release_report.missingFields[recordID].append(field)

            for field, datapoint in record:
                if datapoint.report is None:
                    # We can skip fields with no reports at this point
                    # because the only case where a field should not
                    # have a report after validation is when that report
                    # depends on another field, and that case will be
                    # caught by the missing fields check above.
                    continue

                if datapoint.report.status == ReportScore.SUCCESS:
                    release_report.successCount += 1
                    continue

                if datapoint.report.status == ReportScore.WARNING:
                    release_report.warningCount += 1
                    if recordID not in release_report.warningFields:
                        release_report.warningFields[recordID] = []
                    release_report.warningFields[recordID].append(field)
                    continue

                if datapoint.report.status == ReportScore.FAIL:
                    release_report.failCount += 1
                    if recordID not in release_report.failFields:
                        release_report.failFields[recordID] = []
                    release_report.failFields[recordID].append(field)

        if (
            release_report.missingCount == 0
            and release_report.failCount == 0
            and release_report.warningCount == 0
        ):
            release_report.released = True

            # need to actually set released value in the dataset metadata object in dynamodb

        return format_response(200, release_report)

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
