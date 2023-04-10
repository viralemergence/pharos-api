import os
import json
import boto3
from auth import check_auth
from format import format_response


DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


def lambda_handler(event, _):
    post_data = json.loads(event.get("body", "{}"))
    user = check_auth(post_data["researcherID"])

    if not user:
        return format_response(401, {"message": "Unauthorized"})

    return format_response(200, user.json(), preformatted=True)
