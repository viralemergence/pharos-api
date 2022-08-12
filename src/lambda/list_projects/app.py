import json
import os

import boto3
from auth import check_auth
from format import format_response

DYNAMODB = boto3.resource("dynamodb")
PROJECTS_TABLE = os.environ["PROJECTS_TABLE_NAME"]


def lambda_handler(event, _):
    post_data = json.loads(event.get("body", "{}"))

    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:
        print(
            {
                "Keys": [
                    {"projectID": {"S": projectID}}
                    for projectID in post_data["projectIDs"]
                ]
            }
        )

        projects = DYNAMODB.batch_get_item(
            RequestItems={
                PROJECTS_TABLE: {
                    "Keys": [
                        {"projectID": {"S": projectID}}
                        for projectID in post_data["projectIDs"]
                    ]
                }
            }
        )
        return format_response(200, projects)

    except Exception as e:  # pylint: disable=broad-except
        print(e)
        return format_response(403, e)
