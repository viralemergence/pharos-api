import boto3
import json

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]
CORS_ALLOW = os.environ["CORS_ALLOW"]


def lambda_handler(event, context):

    post_data = json.loads(event.get("body", "{}"))

    try:
        pass
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps({"message": str(e)}),
        }  # This should be logged

    return
