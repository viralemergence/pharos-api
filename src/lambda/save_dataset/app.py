import json
import os

import boto3
from auth import check_auth  # pylint: disable=no-name-in-module
from format import format_response  # pylint: disable=import-error


DYNAMODB = boto3.resource("dynamodb")
USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:
        DATASETS_TABLE.put_item(Item=post_data)

        return format_response(200, "")

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
