from datetime import datetime
import os

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Extra, Field, ValidationError
from auth import check_auth
from format import format_response
from register import Dataset

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


class UploadDatasetBody(BaseModel):
    """Event data payload to upload a dataset."""

    researcher_id: str = Field(..., alias="researcherID")
    dataset: Dataset

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):
    try:
        validated = UploadDatasetBody.parse_raw(event.get("body", "{}"))
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

    next_dataset = validated.dataset

    try:
        project_response = METADATA_TABLE.get_item(
            Key={"pk": validated.dataset.project_id, "sk": validated.dataset.dataset_id}
        )

        if project_response.get("Item"):
            prev_dataset = Dataset.parse_table_item(project_response["Item"])

            if prev_dataset.last_updated and validated.dataset.last_updated:
                prev_updated = datetime.strptime(
                    prev_dataset.last_updated, "%Y-%m-%dT%H:%M:%S.%fZ"
                )

                next_updated = datetime.strptime(
                    validated.dataset.last_updated, "%Y-%m-%dT%H:%M:%S.%fZ"
                )

                # if the incoming update is older than the previous
                # update, keep the previous version
                if next_updated < prev_updated:
                    next_dataset = prev_dataset

        METADATA_TABLE.put_item(Item=next_dataset.table_item())
        return format_response(200, "Dataset saved.")

    except ClientError:
        # if there is an error checking for a previous dataset
        # save the incoming datsaet.
        METADATA_TABLE.put_item(Item=next_dataset.table_item())
        return format_response(200, "Dataset saved.")

    ## Leaving this off to think about later, because it is not
    ## correct according to the new design where this route
    ## could be used to chang the title on a published dataset.

    # # Whenever the dataset is saved via this route, it must
    # # be marked as unreleased; the only way to release it is
    # # via the release route, and if the client-side tries to
    # # set "releaseStatus" it should be overridden.
    # validated.dataset.release_status = DatasetReleaseStatus.UNRELEASED
