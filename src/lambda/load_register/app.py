import json
import os
from tracemalloc import start
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

    query_keys = {
        "KeyConditionExpression": Key("datasetID").eq(post_data["datasetID"]),
        "Limit": 5,
    }

    try:
        register = {}
        done = False
        start_key = None
        while not done:
            if start_key:
                query_keys["ExclusiveStartKey"] = start_key
            response = DATASETS_TABLE.query(**query_keys)
            for row in response["Items"]:
                if row["recordID"] != "_meta":
                    register[row["recordID"]] = row["record"]
            start_key = response.get("LastEvaluatedKey", None)
            done = start_key is None
        return format_response(200, "Succesful download")

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
