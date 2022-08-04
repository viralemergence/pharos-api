import json
import os
import boto3

from auth import check_auth  # pylint: disable=no-name-in-module
from format import format_response  # pylint: disable=import-error

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:
        response = S3CLIENT.list_objects_v2(
            Bucket=DATASETS_S3_BUCKET, Prefix=f'{post_data["datasetID"]}/'
        )

        response["Contents"].sort(key=lambda item: item["LastModified"], reverse=True)

        key = response["Contents"][0]["Key"]

        register = S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)
        return format_response(
            200, {"response": register["Body"].read().decode("UTF-8")}
        )

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
