import boto3
import json
import os
from datetime import datetime
import uuid
import hashlib

DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]

USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


def lambda_handler(event, context):

    post_data = json.loads(event.get("body", "{}"))

    # # Verify researcher id is in USERS_TABLE - Authentication FAKE
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

    # Storing data to S3 BUCKET
    try:

        md5hash = str(hashlib.md5( post_data["rows"].encode("utf-8").hexdigest() ))
        key = f'{post_data["registerID"]}/{md5hash}.json'
        

        response = S3CLIENT.put_object(
            Bucket=DATASETS_S3_BUCKET,
            Key=key,
            Body=(bytes(json.dumps(post_data["rows"]).encode("UTF-8"))),
        )

    except Exception as e:
        return {
            "statusCode": 403,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps(
                {
                    "exception": str(e),
                }
            ),
        }

    # dataset = {"key": key, "date": post_data["date"]}

    # # Update version
    # try:
    #     response = DATASETS_TABLE.update_item(
    #         Key={
    #             "researcherID": post_data["researcherID"],  # Partition Key
    #             "datasetID": post_data["datasetID"],  # Sort Key
    #         },
    #         # Append to version list
    #         UpdateExpression="SET versions = list_append(versions,:d)",
    #         ExpressionAttributeValues={":d": [dataset]},
    #         ReturnValues="UPDATED_NEW",
    #     )

    # except Exception as e:
    #     return {
    #         "statusCode": 500,
    #         "headers": {
    #             "Access-Control-Allow-Origin": CORS_ALLOW,
    #         },
    #         "body": json.dumps({"message": str(e)}),
    #     }  # This should be logged

    # return {
    #     "statusCode": 200,
    #     "headers": {
    #         "Access-Control-Allow-Origin": CORS_ALLOW,
    #     },
    #     "body": json.dumps({"key": key}),
    # }
