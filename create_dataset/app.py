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

    # TODO: Verify researcher id is in USERS_TABLE
    """ try:
        post_data = json.loads(event.get("body", "{}"))

        users_response = USERS_TABLE.get_item(
            Key={"researcherID": post_data["researcherID"]}
        )
    except:
        pass
    """

    post_data = json.loads(event.get("body", "{}"))
    s3location = "s3://something"

    try:
        date = datetime.utcnow()

        response = DATASETS_TABLE.put_item(
            Item = {
                "researcherID" : post_data["researcherID"],
                "datasetID" : int(datetime.timestamp(date)), # Unique to researcher
                "name" : post_data["dataset_name"],
                "samples_taken" : post_data["samples_taken"],
                "detection_run" : post_data["detection_run"],
                "versions" : [
                    {
                        "uri" : s3location,
                        "date" : str(date) # DyanamoDb does not support date types
                    }
                ]
            }
        )
    
    except Exception as e:
        print(e) # This should be logged

    return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps(response),
        }