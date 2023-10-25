"""
NOTE:

    This function is significantly misnamed; this function
    is used to save updated metadata about the user, and
    only used for user creation as a secondary purpose.

"""
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

    try:
        user_response = METADATA_TABLE.get_item(
            Key={"pk": researcher_id, "sk": "_meta"}
        )

        if "Item" in user_response:
            user = User.parse_table_item(user_response["Item"])

            # if the incoming user data has project_ids, we can discard
            # them because this route should not grant access to new
            # projects. The only way to get access to a new project
            # should be to create it using create_project route which
            # verifies that the project is new before adding it to the user.
            if validated.project_ids:
                validated.project_ids = user.project_ids

            # download_ids should be merged by taking the union of both sets
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
