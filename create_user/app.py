import os
import json
import boto3
import uuid


DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]

USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])


def lambda_handler(event, context):

    try:
        post_data = json.loads(event.get("body", "{}"))
        researcherID = uuid.uuid4().hex

        users_response = USERS_TABLE.put_item(
            Item={
                "researcherID": researcherID,
                "organization": post_data["organization"],
                "email": post_data["email"],
                "name": post_data["name"],
            }
        )

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps(
                {
                    "researcherID": researcherID,
                    "table_response": users_response,
                }
            ),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps(
                {
                    "exception": e,
                }
            ),
        }
