import os
from datetime import datetime
from typing import Optional

import boto3
from auth import check_auth
from botocore.utils import ClientError
from format import format_response
from pydantic import BaseModel, Extra, Field, ValidationError
from register import Dataset

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class LoadRecordsEventBody(BaseModel):
    project_id: str = Field(alias="projectID")
    dataset_id: str = Field(alias="datasetID")
    register_page: str = Field(alias="registerPage")
    last_updated: Optional[str] = Field(alias="lastUpdated")

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):

    try:
        user = check_auth(event)
    except ValidationError:
        return format_response(403, "Not Authorized")

    if not user:
        return format_response(403, "Not Authorized")
    if not user.project_ids:
        return format_response(404, "Researcher has no projects")

    try:
        validated = LoadRecordsEventBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    if validated.project_id not in user.project_ids:
        return format_response(403, "Researcher does not have access to this project")

    if validated.last_updated:
        client_last_updated = datetime.strptime(
            validated.last_updated, "%Y-%m-%dT%H:%M:%S.%fZ"
        )

        print("CLIENT LAST UPDATED")
        print(client_last_updated)

        server_last_updated = None

        try:
            dataset_response = METADATA_TABLE.get_item(
                Key={
                    "pk": validated.project_id,
                    "sk": validated.dataset_id,
                }
            )

            dataset = Dataset.parse_table_item(dataset_response["Item"])

            print("DATASET")
            print(dataset)

            if dataset.register_pages and dataset.register_pages.get(
                validated.register_page
            ):
                server_last_updated = datetime.strptime(
                    dataset.register_pages[validated.register_page].last_updated,
                    "%Y-%m-%dT%H:%M:%S.%fZ",
                )

            print("SERVER LAST UPDATED")
            print(server_last_updated)

        except ClientError as e:
            pass

        # If the client's last_updated timestamp matches or is
        # newer than the one on the server, we can just return
        # an empty register because the client already has the
        # latest version of all records in this page.
        if (
            server_last_updated is not None
            and client_last_updated >= server_last_updated
        ):
            return format_response(200, {"register": {}})

    # If the server has a newer version of the page, send that.

    # There is a potential optimization here to only send records
    # which contain datapoints with versions newer than the
    # client_last_updated timestamp, which would cost more
    # server time but potentially save a lot of network time.
    # At the moment users are not likely to hit the case where
    # they need to sync just a few records, because not many
    # (or maybe any) users users are currently using multiple
    # devices for data entry simultaneously so I'm leaving
    # things simple here. The main use-case where users will
    # hit this is if they log out and log back in or log in
    # to a new device, and in those cases they need the whole
    # page anyway.

    print("SEND FULL REGISTER")

    key = f"{validated.dataset_id}/data_{validated.register_page}.json"

    try:
        register_json = (
            S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)["Body"]
            .read()
            .decode("utf-8")
        )
        return format_response(200, register_json, preformatted=True)

    except ClientError as e:
        return format_response(500, e)
