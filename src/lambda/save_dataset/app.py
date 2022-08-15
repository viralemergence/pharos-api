import json
import os

import boto3
from auth import check_auth
from format import format_response

DYNAMODB = boto3.resource("dynamodb")
PROJECTS_TABLE = DYNAMODB.Table(os.environ["PROJECTS_TABLE_NAME"])


def lambda_handler(event, _):
    post_data = json.loads(event.get("body", "{}"))

    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:
        # Append to set of datasetIDs
        PROJECTS_TABLE.update_item(
            Key={"projectID": post_data["projectID"]},
            # Dynamodb docs specify ADD for sets
            UpdateExpression="ADD datasetIDs :i",
            # Need to indicate it is a string set - SS
            ExpressionAttributeValues={":i": set([post_data["datasetID"]])},
        )

        return format_response(200, "")

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
