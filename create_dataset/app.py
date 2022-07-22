import json
import os

import boto3

# TODO: Change name to save_dataset, don't forget to change in template.yaml

DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]

USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    # Verify researcher id is in USERS_TABLE
    try:
        users_response = USERS_TABLE.get_item(
            Key={"researcherID": post_data["researcherID"]}
        )

        # Exit if user is not in the database.
        if "Item" not in users_response:
            return {
                "statusCode": 403,
                "headers": {
                    "Access-Control-Allow-Origin": CORS_ALLOW,
                },
                "body": json.dumps({"message": "User does not exist"}),
            }

    except Exception as e:  # pylint: disable=broad-except
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps({"message": str(e)}),
        }  # This should be logged

    try:
        response = DATASETS_TABLE.put_item(Item=post_data)

        return {
            "statusCode": response["ResponseMetadata"]["HTTPStatusCode"],
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
        }

    except Exception as e:  # pylint: disable=broad-except
        return {
            "statusCode": 403,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps({"message": str(e)}),
        }  # This should be logged
