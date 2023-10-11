import os
import boto3
from botocore.utils import ClientError
from pydantic import BaseModel, Extra, Field, ValidationError

from auth import check_auth
from format import format_response

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class LoadRegisterBody(BaseModel):
    """Event data payload to load a dataset register."""

    project_id: str = Field(..., alias="projectID")
    dataset_id: str = Field(..., alias="datasetID")

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):

    try:
        user = check_auth(event)
    except ValidationError:
        return format_response(403, "Not Authorized")

    if not user:
        return format_response(403, "Not Authorized")
    if not user.project_ids:
        return format_response(404, "Researcher has no projects")

    try:
        validated = LoadRegisterBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    if validated.project_id not in user.project_ids:
        return format_response(403, "Researcher does not have access to this project")

    try:

        key = f"{validated.dataset_id}/data.json"
        register_json = (
            S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)["Body"]
            .read()
            .decode("utf-8")
        )
        return format_response(200, register_json, preformatted=True)

    except ClientError as e:
        return format_response(500, e)


## DynamoDB implmentation
# def lambda_handler(event, _):

#     post_data = json.loads(event.get("body", "{}"))

#     authorized = check_auth(post_data["researcherID"])
#     if not authorized:
#         return format_response(403, "Not Authorized")

#     query_keys = {
#         "KeyConditionExpression": Key("datasetID").eq(post_data["datasetID"]),
#         "Limit": 5,
#     }

#     try:
#         register = {}
#         done = False
#         start_key = None
#         while not done:
#             if start_key:
#                 query_keys["ExclusiveStartKey"] = start_key
#             response = DATASETS_TABLE.query(**query_keys)
#             for row in response["Items"]:
#                 if row["recordID"] != "_meta":
#                     register[row["recordID"]] = row["record"]
#             start_key = response.get("LastEvaluatedKey", None)
#             done = start_key is None
#         return format_response(200, "Succesful download")

#     except Exception as e:  # pylint: disable=broad-except
#         return format_response(403, e)
