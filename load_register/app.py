import json
import os
import boto3

# This import is recognized by linting
# You can run this function locally
# Not recognized by aws, because the template specifies the path as app.lambda_handler
from ..utils import utils

S3CLIENT = boto3.client("s3")
DYNAMODB = boto3.resource("dynamodb")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]
CORS_ALLOW = os.environ["CORS_ALLOW"]
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])
USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])


# This commented section will be deleted

# def format_response(code, body):
#     return {
#         "statusCode": code,
#         "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
#         "body": json.dumps(body),
#     }


# def check_auth(researcherID):
#     # Verify researcher id is in USERS_TABLE - Authentication FAKE
#     try:
#         users_response = USERS_TABLE.get_item(Key={"researcherID": researcherID})
#         # Exit if user is not in the database.
#         if "Item" not in users_response:
#             return False

#     except Exception:  # pylint: disable=broad-except
#         return False

#     return True


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    authorized = utils.check_auth(post_data["researcherID"])
    if not authorized:
        return utils.format_response(403, "Not Authorized")

    try:
        response = S3CLIENT.list_objects_v2(
            Bucket=DATASETS_S3_BUCKET, Prefix=f'{post_data["datasetID"]}/'
        )

        response["Contents"].sort(key=lambda item: item["LastModified"], reverse=True)

        key = response["Contents"][0]["Key"]

        register = S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)
        return utils.format_response(
            200, {"response": register["Body"].read().decode("UTF-8")}
        )

    except Exception as e:  # pylint: disable=broad-except
        return utils.format_response(403, e)
