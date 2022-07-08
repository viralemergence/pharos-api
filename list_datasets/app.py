import boto3
from boto3.dynamodb.conditions import Key
import json
import os
from decimal import Decimal

DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]

USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def lambda_handler(event, context):

    post_data = json.loads(event.get("body", "{}"))

    try:
        # This might have to change because it only queries by page (?).
        response = DATASETS_TABLE.query(
            KeyConditionExpression=Key("researcherID").eq(
                post_data["researcherID"]
            )  # Query only by partition key
        )

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps({"message": str(e)}),
        }  # This should be logged

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": CORS_ALLOW,
        },
        "body": json.dumps(response["Items"], cls=DecimalEncoder
        ),  # Returns a dictionary with a list of dataset in a project
        # this functionality will be change to datasets per project
    }
