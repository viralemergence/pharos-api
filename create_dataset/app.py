import boto3
import json
import os
from datetime import datetime

DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]

USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


def lambda_handler(event, context):

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

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps({"message": str(e)}),
        }  # This should be logged

    try:
        date = datetime.utcnow()  # Date should be standardized to UTC

        # Create a unique timestamp for dataset id. Could be repeated for different researchers
        datasetid = int(datetime.timestamp(date))

        item = {
            "datasetID": datasetid,
            **post_data,
            "versions": [],
        }

        response = DATASETS_TABLE.put_item(Item=item)

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps(item),
        }

    except Exception as e:
        return {
            "statusCode": 403,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps({"message": str(e)}),
        }  # This should be logged
