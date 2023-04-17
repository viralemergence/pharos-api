import os
import boto3
from botocore.exceptions import ClientError

from register import User

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


def check_auth(researcher_id):
    try:
        users_response = METADATA_TABLE.get_item(
            Key={"pk": researcher_id, "sk": "_meta"}
        )

    except ClientError:
        return False

    if "Item" not in users_response:
        return False

    user = User.parse_table_item(users_response["Item"])
    return user
