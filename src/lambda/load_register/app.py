import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from auth import check_auth
from format import format_response

DYNAMODB = boto3.resource("dynamodb")
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:

        response = DATASETS_TABLE.query(
            KeyConditionExpression=Key("datasetID").eq(post_data["datasetID"])
        )

        register = {}
        for row in response["Items"]:
            if row["recordID"] != "_meta":
                register[row["recordID"]] = row["record"]

        return format_response(200, register)

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
