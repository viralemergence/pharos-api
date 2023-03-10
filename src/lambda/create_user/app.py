import json
import os
import uuid
import boto3
from format import format_response

DYNAMODB = boto3.resource("dynamodb")
USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])


def lambda_handler(event, _):
    try:
        post_data = json.loads(event.get("body", "{}"))

        # If resercherID is not provided, generate a new one
        researcherID = post_data.get("researcherID")
        if not researcherID:
            researcherID = uuid.uuid4().hex

        # TODO
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
