import boto3
import json
import uuid
import os

S3CLIENT = boto3.client("s3")
CORS_ALLOW = os.environ["CORS_ALLOW"]


def lambda_handler(event, context):

    post_data = json.loads(event.get("body", "{}"))

    try:
        key = str(uuid.uuid3(uuid.uuid4(), post_data["researcherID"])) + ".json"

        response = S3CLIENT.Object(
            post_data["bucket"],  # Bucket name
            key,  # Key: s3://veryrandomlocation.json
        ).put(Body=post_data["file"])

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
