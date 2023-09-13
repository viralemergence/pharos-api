import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Extra, Field, ValidationError

from auth import check_auth
from format import format_response
from register import Project


DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


class SaveProjectBody(BaseModel):
    """Event data payload to save a project."""

    researcher_id: str = Field(..., alias="researcherID")
    project: Project

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):
    try:
        validated = SaveProjectBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    user = check_auth(validated.researcher_id)

    if not user:
        return format_response(403, "Not Authorized")

    try:
        project_response = METADATA_TABLE.get_item(
            Key={"projectID": validated.project.project_id}
        )

        if project_response.get("Item"):
            prev_project = Project.parse_table_item(project_response["Item"])

            # Check to make sure this researcher is an author on the project
            if prev_project.authors and not user.researcher_id in [
                author.researcher_id for author in prev_project.authors
            ]:
                return format_response(403, "Not Authorized")

            if prev_project.last_updated and validated.project.last_updated:
                prev_updated = datetime.fromisoformat(prev_project.last_updated)
                next_updated = datetime.fromisoformat(validated.project.last_updated)

                if next_updated > prev_updated:
                    try:
                        METADATA_TABLE.put_item(Item=validated.project.table_item())
                        return format_response(200, "Succesful upload")

                    except ClientError as e:
                        return format_response(500, e)
            else:
                return format_response(200, "Newer updates exist")

    except ClientError as e:
        # If the project does not already exist,
        # we can skip ahead to creating it
        pass

    try:
        METADATA_TABLE.put_item(Item=validated.project.table_item())
        return format_response(200, "Succesful upload")

    except ClientError as e:
        return format_response(500, e)
