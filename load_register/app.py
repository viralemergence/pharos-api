import boto3
import json
import os

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]
CORS_ALLOW = os.environ["CORS_ALLOW"]


def lambda_handler(event, context):

    post_data = post_data = json.loads(event.get("body", "{}"))

    try:

        key = f'{post_data["registerID"]}/{post_data["key"]}'

        response = S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps({"message": str(e)}),
        }  # This

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": CORS_ALLOW,
        },
        "body": json.dumps({"response": response["Body"].read().decode("UTF-8")}),
    }
