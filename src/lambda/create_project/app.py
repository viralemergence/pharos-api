import json
import os

import boto3
from auth import check_auth
from botocore.exceptions import ClientError
from format import format_response
from pydantic import BaseModel, Extra, ValidationError
from register import Project

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


class CreateProjectBody(BaseModel):
    """Event data payload to create a project."""

    project: Project

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):
    try:
        user = check_auth(event)

    except ValidationError:
        return format_response(401, {"message": "Unauthorized"})

    if not user:
        return format_response(401, {"message": "Unauthorized"})

    try:
        validated = CreateProjectBody.parse_obj(json.loads(event.get("body", "{}")))
    except ValidationError as e:
        print(e.json(indent=2))
        return format_response(400, e.json())

    # check if project already exists in metadata table
    try:
        project_response = METADATA_TABLE.get_item(
            Key={"pk": validated.project.project_id, "sk": "_meta"}
        )

        if "Item" in project_response:
            return format_response(403, "Project already exists")

    except ClientError as e:
        try:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":  # type: ignore
                pass
            else:
                return format_response(500, e)
        except KeyError:
            return format_response(500, e)

    # if it does not exist, save it and add it to the user's project_ids
    try:
        METADATA_TABLE.put_item(Item=validated.project.table_item())

        if not user.project_ids:
            user.project_ids = set()
        user.project_ids.add(validated.project.project_id)
        METADATA_TABLE.put_item(Item=user.table_item())

        return format_response(200, "Project created")

    except ClientError as e:
        return format_response(500, e)
