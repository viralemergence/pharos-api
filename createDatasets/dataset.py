from asyncio.log import logger
import logging
import boto3
import json
from datetime import datetime


CLIENT = boto3.resource('dynamodb')
DATASET_TABLE = CLIENT.Table(os.environ["DATASET_TABLE_NAME"])


def lambda_handler(request):
    datasetid = str() 
    date = datetime.utcnow()

    try:
        response = DATASET_TABLE.put_item(
            Item = {
                datasetid : {
                    "name" : request["name"],
                    "samples_taken" : request["samples_taken"],
                    "detection_run" : request["detection_run"],
                    "versions" : [
                        {
                            "uri" : s3location, # TODO
                            "date" : date
                        }
                    ]
                }
            }
        )
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps({"exception": e}),
        }

    return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": {
                "researcherid" : researcherid,
                "date" : date
            }
        }


def lambda_handler(event, context):
    """
    

    """

    # Create a json file from event
    ## NOT THIS
    with open( name + ".json", "w" ) as file:
        json.dumps(event)

    # Upload to S3 bucket
    uploadS3(file, BUCKET)
    logging.info(file + "uploaded to S3 Bucket") # Change message to something more meaningful

    # Then store in dynamodb
    storeDynamodb(researcher, name)
    logging.info(researcher + "-" + name + " saved") # Change messages to something more meaningful

    return { # Change essage to the user
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps(
                {
                    "token": "12-309oij13120-8254",
                    "username": "dev user",
                    "email": "devs@talusanalytics.com",
                    "researcherID": "5431",
                }
            ),
        }


def storeDynamodb(researcherID:str, fileName:str) -> bool:

    client = boto3.resource('dynamodb')
    table = client.Table() # Define this later

    try:
        response = table.put_item(
            Item = {
                "ResearcherID" : researcherID, # Verify when db is created
                "File": fileName # Verify when db is created
            }
        )
    except:
        return False
    
    return True


def uploadS3(json, bucket) -> bool:

    client = boto3.client("s3")

    try:
        response = client.upload_file(json, bucket)
    except ClientError:
        return False
    except FileNotFoundError:
        return False
    except NoCredentialsError:
        return False

    return True

    def createDataset():
        pass