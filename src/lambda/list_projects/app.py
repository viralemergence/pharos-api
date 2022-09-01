import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from auth import check_auth
from format import format_response

DYNAMODB = boto3.resource("dynamodb")
USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
PROJECTS_TABLE = os.environ["PROJECTS_TABLE_NAME"]


def lambda_handler(event, _):
    post_data = json.loads(event.get("body", "{}"))

    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:

        user = USERS_TABLE.get_item(Key={"researcherID": post_data["researcherID"]})
        projectids = user["Item"]["projectIDs"]

        projects = DYNAMODB.batch_get_item(
            RequestItems={
                PROJECTS_TABLE: {
                    "Keys": [{"projectID": projectID} for projectID in projectids]
                }
            }
        )
        return format_response(200, projects["Responses"][PROJECTS_TABLE])

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
