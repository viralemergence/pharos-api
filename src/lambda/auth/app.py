import os
import json
import boto3
from botocore.exceptions import ClientError
from format import format_response


DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


def lambda_handler(event, _):
    post_data = json.loads(event.get("body", "{}"))
    try:
        users_response = METADATA_TABLE.get_item(
            Key={"pk": post_data["researcherID"], "sk": "_meta"}
        )

    except ClientError as e:
        return format_response(500, e)

    if "Item" in users_response:
        return format_response(200, users_response["Item"])

    return format_response(500, {"message": "User does not exist"})
