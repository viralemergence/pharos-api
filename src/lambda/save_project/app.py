import os

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Extra, ValidationError

from auth import check_auth
from format import format_response
from register import Project


DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


class SaveProjectBody(BaseModel):
    """Event data payload to save a project."""

    researcherID: str
    project: Project

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):
    try:
        validated = SaveProjectBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    user = check_auth(validated.researcherID)

    # This should also check if the user has permission to
    # edit this project, but to add that we need to create
    # a dedicated route for creating projects because when
    # a project is created, the user is updated with the new
    # projectID at the same time as the save_project lambda
    # is called. Those two updates need to be in the same
    # api route in the case of new project creation.
    if not user:
        return format_response(403, "Not Authorized")

    # in the future need to query the existing project (if it
    # exists) and merge the new data with the old data

    try:
        METADATA_TABLE.put_item(Item=validated.project.table_item())
        return format_response(200, "Succesful upload")

    except ClientError as e:
        return format_response(500, e)
