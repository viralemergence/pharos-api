import boto3
import json
import os
import uuid
from datetime import datetime


DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]

USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])

def lambda_handler(event, context):

    post_data = json.loads(event.get("body", "{}"))
    
    try:

        response = DATASETS_TABLE.query(
            KeyConditionExpression  = "researcherID = :researcherID",
            ExpressionAttributeValues = {":researcherID" : {":r" : post_data["researcherID"]}}
        )
    
    except Exception as e:
        print(e)

    return { # Change essage to the user
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": response["item"] # I think this is an iterable
        }