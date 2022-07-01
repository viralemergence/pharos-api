import os
import json
import uuid
import boto3


DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]

USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


def lambda_handler(event, context):
    print("HELLO WORLD")

    # Double check user exists
    # users_response = USERS_TABLE.get_item(Key={"researcherID": "109312098lkjasdf"})

    resercherID = uuid.uuid4().hex

    datasets_table_response = DATASETS_TABLE.put_item(
        Item={"researcherID": resercherID, "name": "test data"}
    )

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": CORS_ALLOW,
        },
        "body": json.dumps(datasets_table_response),
    }
