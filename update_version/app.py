import boto3
import json
import uuid
import os
from io import StringIO


S3CLIENT = boto3.client("s3")
CORS_ALLOW = os.environ["CORS_ALLOW"]
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]

# Update dataset
def lambda_handler(event, context):

    post_data = json.loads(event.get("body", "{}"))

    try:

        # file = StringIO()
        # json.dump(post_data["raw"], file)

        key = str(uuid.uuid3(uuid.uuid4(), post_data["researcherID"])) + ".json"

        response = S3CLIENT.put_object(
            Bucket=DATASETS_S3_BUCKET,
            Key=key,
            Body=(bytes(json.dumps(post_data["raw"]).encode("UTF-8"))),
        )

    except Exception as e:
        return {
            "statusCode": 403,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps(
                {
                    "exception": str(e),
                }
            ),
        }

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": CORS_ALLOW,
        },
        "body": json.dumps({"key": key}),
    }
