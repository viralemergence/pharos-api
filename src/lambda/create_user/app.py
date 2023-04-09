import os
import uuid
import boto3
from botocore.exceptions import ClientError

from pydantic import ValidationError

from format import format_response
from register import User

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


def lambda_handler(event, _):
    try:
        validated = User.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    # If resercherID is not provided, generate a new one
    researcherID = validated.researcherID
    if not researcherID:
        researcherID = uuid.uuid4().hex

    user_dict = validated.dict()

    # remove researcherID from the dict and use it as partition key
    user_dict["pk"] = user_dict.pop("researcherID")
    # sort key is just _meta
    user_dict["sk"] = "_meta"

    # Need to add handling merging of the user's list of
    # projects here, because for now an out-of-sync client
    # could write a project list which is missing projects
    # created on another client by the same user.

    try:
        users_response = METADATA_TABLE.put_item(Item=user_dict)
        return format_response(
            200,
            {
                "researcherID": researcherID,
                "table_response": users_response,
            },
        )

    except ClientError as e:
        return format_response(500, e)
