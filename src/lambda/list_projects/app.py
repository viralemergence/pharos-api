import json
import os
import boto3
from botocore.utils import ClientError
from pydantic import BaseModel, ValidationError
from auth import check_auth
from format import format_response

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = os.environ["METADATA_TABLE_NAME"]


class ListProjectsBody(BaseModel):
    """Event data payload to list projects."""

    researcherID: str


def lambda_handler(event, _):
    try:
        validated = ListProjectsBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    user = check_auth(validated.researcherID)
    if not user:
        return format_response(403, "Not Authorized")

    if not user.projectIDs:
        return format_response(200, [])

    try:
        projects = DYNAMODB.batch_get_item(
            RequestItems={
                METADATA_TABLE: {
                    "Keys": [{"projectID": projectID} for projectID in user.projectIDs]
                }
            }
        )
        return format_response(200, projects["Responses"][METADATA_TABLE])

    except ClientError as e:
        return format_response(500, e)
