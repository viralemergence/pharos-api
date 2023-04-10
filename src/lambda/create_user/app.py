import os
import uuid
import json

import boto3
from botocore.exceptions import ClientError
from pydantic import ValidationError

from format import format_response
from register import User

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


def lambda_handler(event, _):
    try:
        user_data = json.loads(event.get("body", "{}"))
        if not "researcherID" in user_data:
            user_data["researcherID"] = "res" + uuid.uuid4().hex

        validated = User.parse_obj(user_data)

    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    # Need to add handling merging of the user's list of
    # projects here, because for now an out-of-sync client
    # could write a project list which is missing projects
    # created on another client by the same user.

    try:
        users_response = METADATA_TABLE.put_item(Item=validated.table_item())
        return format_response(
            200,
            {
                "researcherID": validated.researcherID,
                "table_response": users_response,
            },
        )

    except ClientError as e:
        return format_response(500, e)
