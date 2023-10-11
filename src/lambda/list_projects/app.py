import os
import boto3
from botocore.utils import ClientError
from pydantic import ValidationError
from auth import check_auth
from format import format_response

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = os.environ["METADATA_TABLE_NAME"]


def lambda_handler(event, _):
    try:
        user = check_auth(event)
    except ValidationError:
        return format_response(403, "Not Authorized")

    if not user:
        return format_response(403, "Not Authorized")

    if not user.project_ids:
        return format_response(200, [])

    try:
        projects = DYNAMODB.batch_get_item(
            RequestItems={
                METADATA_TABLE: {
                    "Keys": [
                        {"pk": projectID, "sk": "_meta"}
                        for projectID in user.project_ids
                    ]
                }
            }
        )

        # rename pk to projectID and drop the sk
        for project in projects["Responses"][METADATA_TABLE]:
            project["projectID"] = project.pop("pk")
            project.pop("sk")

        return format_response(200, projects["Responses"][METADATA_TABLE])

    except ClientError as e:
        return format_response(500, e)
