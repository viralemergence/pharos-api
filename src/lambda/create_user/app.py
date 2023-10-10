import os
import json

import boto3
from botocore.exceptions import ClientError
from pydantic import ValidationError
from auth import Claims

from format import format_response
from register import User

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


def lambda_handler(event, _):
    try:
        claims = Claims.parse_obj(
            event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
        )
        researcher_id = f"res{claims.sub}"

    except ValidationError as e:
        return format_response(401, e.json())

    user_data = json.loads(event.get("body", "{}"))
    user_data["researcherID"] = researcher_id

    try:
        validated = User.parse_obj(user_data)

    except ValidationError as e:
        print(e.json(indent=2))
        return format_response(400, e.json())

    # Need to add handling merging of the user's list of
    # projects here, because for now an out-of-sync client
    # could write a project list which is missing projects
    # created on another client by the same user.

    try:
        users_response = METADATA_TABLE.put_item(Item=validated.table_item())
        print(users_response)
        return format_response(
            200,
            {}
            # {
            #     "researcherID": validated.researcher_id,
            #     "table_response": users_response,
            # },
        )

    except ClientError as e:
        return format_response(500, e)
