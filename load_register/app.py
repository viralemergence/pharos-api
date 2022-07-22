import boto3
import json
import os

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]
CORS_ALLOW = os.environ["CORS_ALLOW"]
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])

def lambda_handler(event, context):

    post_data = post_data = json.loads(event.get("body", "{}"))

    # Verify researcherID and datasetID exist
    try:
        users_response = DATASETS_TABLE.get_item(
            Key={"researcherID": post_data["researcherID"],
            "datasetID": post_data["datasetID"]}
        )

        # Exit if user is not in the database.
        if "Item" not in users_response:
            return {
                "statusCode": 403,
                "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
                "body": json.dumps({"message": "Dataset does not exist"}),
            }

    except Exception as e:  # pylint: disable=broad-except
        return {
            "statusCode": 500,
            "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
            "body": json.dumps({"message": str(e)}),
        }  # This should be logged


    try:
        response = S3CLIENT.list_objects_v2(
            Bucket=DATASETS_S3_BUCKET,
            prefix= f'{post_data["datasetID"]}/'
        )

        key = response['Contents'].sort(key=lambda item:item['LastModified'], reverse=True)[0]['Key']

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
