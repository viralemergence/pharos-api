import json
import os

import boto3

# from utils import format_response, check_auth - TODO

# TODO: Change name to save_dataset, don't forget to change in template.yaml

DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]

USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


def format_response(code, body):
    return {
        "statusCode": code,
        "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
        "body": json.dumps(body),
    }


def check_auth(researcherID):
    # Verify researcher id is in USERS_TABLE - Authentication FAKE
    try:
        users_response = USERS_TABLE.get_item(Key={"researcherID": researcherID})
        # Exit if user is not in the database.
        if "Item" not in users_response:
            return False

    except Exception:  # pylint: disable=broad-except
        return False

    return True


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
