import json
import os
import uuid
import boto3
from pydantic import BaseModel
from format import format_response

DYNAMODB = boto3.resource("dynamodb")
USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])


class CreateUserData(BaseModel):
    """Data model for the create user request"""

    researcherID: str
    organization: str
    email: str
    name: str


class Event(BaseModel):
    """Data model for the event payload"""

    body: str


def lambda_handler(event, _):
    try:
        post_data = json.loads(event.get("body", "{}"))

        # If resercherID is not provided, generate a new one
        researcherID = post_data.get("researcherID")
        if not researcherID:
            researcherID = uuid.uuid4().hex

        # This overwrites the existing record completely
        # What we actually need here is a merge; so that
        # an out of date client can't take away project
        # permissions or roll back other data.
        users_response = USERS_TABLE.put_item(
            Item={
                "researcherID": researcherID,
                "organization": post_data["organization"],
                "email": post_data["email"],
                "name": post_data["name"],
            }
        )

        return format_response(
            200,
            {
                "researcherID": researcherID,
                "table_response": users_response,
            },
        )

    except Exception as e:  # pylint: disable=broad-except
        return format_response(500, e)
