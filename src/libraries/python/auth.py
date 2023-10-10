import os
import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Field

from register import User

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


class Claims(BaseModel):
    sub: str
    email_verified: str
    iss: str
    cognito_username: str = Field(alias="cognito:username")
    origin_jti: str
    aud: str
    event_id: str
    token_use: str
    auth_time: str
    exp: str
    iat: str
    jti: str
    email: str


def check_auth(lambda_event):

    claims = Claims.parse_obj(
        lambda_event.get("requestContext", {}).get("authorizer", {}).get("claims", {})
    )

    researcher_id = f"res{claims.sub}"

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
