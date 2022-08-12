import os
import json
import boto3
from format import format_response


DYNAMODB = boto3.resource("dynamodb")
USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])


def lambda_handler(event, _):
    post_data = json.loads(event.get("body", "{}"))
    try:
        users_response = USERS_TABLE.get_item(
            Key={"researcherID": post_data["researcherID"]}
        )

    except Exception as e:  # pylint: disable=broad-except
        return format_response(500, e)

    if "Item" in users_response:
        return format_response(200, users_response["Item"])

    return format_response(500, {"message": "User does not exist"})
