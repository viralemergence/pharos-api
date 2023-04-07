import os

import boto3
from pydantic import BaseModel, ValidationError
from auth import check_auth
from format import format_response
from register import Dataset, DatasetReleaseStatus

DYNAMODB = boto3.resource("dynamodb")
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


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

    authorized = check_auth(validated.researcherID)
    if not authorized:
        return format_response(403, "Not Authorized")

    try:
        # Whenever the dataset is saved via this route, it must
        # be marked as unreleased; the only way to release it is
        # via the release route, and if the client-side tries to
        # set "releaseStatus" it should be overridden.
        validated.dataset.releaseStatus = DatasetReleaseStatus.UNRELEASED

        # store in datasets table as a row with
        # the "_meta" special-case sort key
        DATASETS_TABLE.put_item(
            Item={
                **validated.dataset.dict(),
                "recordID": "_meta",
            }
        )

        return format_response(200, "Succesful Upload")

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
