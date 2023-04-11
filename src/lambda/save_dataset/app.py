import os

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, ValidationError
from auth import check_auth
from format import format_response
from register import Dataset, DatasetReleaseStatus

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


class UploadDatasetBody(BaseModel):
    """Event data payload to upload a dataset."""

    researcherID: str
    dataset: Dataset


def lambda_handler(event, _):
    try:
        validated = UploadDatasetBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    user = check_auth(validated.researcherID)

    # check if the user is valid and has access to the project
    if (
        not user
        or not user.projectIDs
        or not validated.dataset.projectID in user.projectIDs
    ):
        return format_response(403, "Not Authorized")

    # Whenever the dataset is saved via this route, it must
    # be marked as unreleased; the only way to release it is
    # via the release route, and if the client-side tries to
    # set "releaseStatus" it should be overridden.
    validated.dataset.releaseStatus = DatasetReleaseStatus.UNRELEASED

    try:
        METADATA_TABLE.put_item(Item=validated.dataset.table_item())
        return format_response(200, "Dataset saved.")

    except ClientError as e:
        print(e)
        return format_response(403, "Error saving dataset")
