from decimal import Decimal
import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from format import format_response  # pylint: disable=import-error


DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    try:
        # This might have to change because it only queries by page (?).
        response = DATASETS_TABLE.query(
            KeyConditionExpression=Key("researcherID").eq(
                post_data["researcherID"]
            )  # Query only by partition key
        )

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps(
                response["Items"], cls=DecimalEncoder
            ),  # Returns a dictionary with a list of dataset in a project
            # this functionality will be change to datasets per project
        }

    except Exception as e:  # pylint: disable=broad-except
        return format_response(500, {"message": str(e)})
