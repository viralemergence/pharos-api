import boto3
import json
import os
from datetime import datetime

DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]

USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])

# from typing import Any, TypedDict

# class Headers(TypedDict):
#     "Access-Control-Allow-Origin": str

# class POST(TypedDict):
#     statusCode: int
#     headers: Any
#     body: Any


def lambda_handler(event, context):  #  -> "POST":

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

    # TODO: This section needs to be integrated
    s3location = "s3://something"  # TODO
    dataset = {"uri": "s3location", "date": str(datetime.utcnow())}

    try:
        response = DATASETS_TABLE.update_item(
            Key={
                "researcherID": post_data["researcherID"],
                "datasetID": post_data["datasetID"],
            },
            UpdateExpression="SET versions = list_append(versions,:d)",
            ExpressionAttributeValues={":d": [dataset]},
            ReturnValues="UPDATED_NEW",
        )

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps({"message": str(e)}),
        }  # This should be logged

    return {  # Change essage to the user
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": CORS_ALLOW,
        },
        "body": json.dumps({"dataset": dataset}),
    }
