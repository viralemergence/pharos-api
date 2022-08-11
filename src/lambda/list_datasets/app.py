from decimal import Decimal
import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from auth import check_auth  # pylint: disable=no-name-in-module
from format import format_response  # pylint: disable=import-error


DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])
PROJECTS_TABLE = DYNAMODB.Table(os.environ["PROJECTS_TABLE_NAME"])


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:

        user_response = PROJECTS_TABLE.get_item(
            Key={"projectID": post_data["projectID"]}
        )

        return format_response(200, user_response["Item"]["datasets"])

    except Exception as e:
        return format_response(403, str(e))

    ## Old code - might reuse it for branch

    # try:
    #     # This might have to change because it only queries by page (?).
    #     response = DATASETS_TABLE.query(
    #         KeyConditionExpression=Key("researcherID").eq(
    #             post_data["researcherID"]
    #         )  # Query only by partition key
    #     )

    #     return {
    #         "statusCode": 200,
    #         "headers": {
    #             "Access-Control-Allow-Origin": CORS_ALLOW,
    #         },
    #         "body": json.dumps(
    #             response["Items"], cls=DecimalEncoder
    #         ),  # Returns a dictionary with a list of dataset in a project
    #         # this functionality will be change to datasets per project
    #     }

    # except Exception as e:  # pylint: disable=broad-except
    #     return format_response(500, {"message": str(e)})
