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
        user_response = METADATA_TABLE.get_item(
            Key={"pk": researcher_id, "sk": "_meta"}
        )

        if "Item" in user_response:
            user = User.parse_table_item(user_response["Item"])

            if user.project_ids:
                if validated.project_ids:
                    validated.project_ids.update(user.project_ids)
                else:
                    validated.project_ids = user.project_ids

            if user.download_ids:
                if validated.download_ids:
                    validated.download_ids.update(user.download_ids)
                else:
                    validated.download_ids = user.download_ids

    except ClientError as e:
        # if there's no prior user, record don't merge
        pass

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
