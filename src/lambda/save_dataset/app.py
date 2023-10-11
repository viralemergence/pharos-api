from datetime import datetime
import os

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Extra, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from auth import check_auth
from engine import get_engine
from format import format_response
from models import PublishedDataset
from register import Dataset, DatasetReleaseStatus

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


class UploadDatasetBody(BaseModel):
    """Event data payload to upload a dataset."""

    dataset: Dataset

    class Config:
        extra = Extra.forbid


def update_publised_dataset(dataset: Dataset):
    engine = get_engine()

    with Session(engine) as session:
        published_dataset = session.scalar(
            select(PublishedDataset).where(
                PublishedDataset.dataset_id == dataset.dataset_id
            )
        )

        if not published_dataset:
            raise ValueError("Dataset not found in database")

        published_dataset.name = dataset.name
        print(f"updating published dataset name to {dataset.name}")

        session.commit()


def lambda_handler(event, _):
    try:
        user = check_auth(event)
    except ValidationError:
        return format_response(403, "Not Authorized")

    if not user:
        return format_response(403, "Not Authorized")
    if not user.project_ids:
        return format_response(403, "Researcher has no projects")

    try:
        validated = UploadDatasetBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    # check if the user is valid and has access to the project
    if not validated.dataset.project_id in user.project_ids:
        return format_response(403, "User does not have access to this project")

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

                print("\n\n")
                print("prev_dataset release_status")
                print(prev_dataset.release_status)

                if prev_dataset.release_status == DatasetReleaseStatus.PUBLISHED:
                    update_publised_dataset(next_dataset)

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
