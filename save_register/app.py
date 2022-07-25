import hashlib
import json
import os

import boto3

DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]

USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


foo = "foo"
if foo:
    pass


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    # # Verify researcher id is in USERS_TABLE - Authentication FAKE
    try:
        users_response = USERS_TABLE.get_item(
            Key={"researcherID": post_data["researcherID"]}
        )

        # Exit if user is not in the database.
        if "Item" not in users_response:
            return {
                "statusCode": 403,
                "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
                "body": json.dumps({"message": "User does not exist"}),
            }

    except Exception as e:  # pylint: disable=broad-except
        return {
            "statusCode": 500,
            "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
            "body": json.dumps({"message": str(e)}),
        }  # This should be logged

    # Storing data to S3 BUCKET
    try:

        # in a variable since we'll use it twice
        data_string = bytes(json.dumps(post_data["data"]).encode("UTF-8"))

        md5hash = str(hashlib.md5(data_string).hexdigest())
        key = f'{post_data["datsetID"]}/{md5hash}.json'

        response = S3CLIENT.put_object(
            Bucket=DATASETS_S3_BUCKET,
            Body=(data_string),
            Key=key,
        )

        return {
            # sending the status code on so that the
            # response actually reflects the success of saving
            "statusCode": response["ResponseMetadata"]["HTTPStatusCode"],
            "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
        }

    except Exception as e:  # pylint: disable=broad-except
        return {
            "statusCode": 403,
            "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
            "body": json.dumps({"exception": str(e)}),
        }

    # dataset = {"key": key, "date": post_data["date"]}

    # # Update version
    # try:
    #     response = DATASETS_TABLE.update_item(
    #         Key={
    #             "researcherID": post_data["researcherID"],  # Partition Key
    #             "datasetID": post_data["datasetID"],  # Sort Key
    #         },
    #         # Append to version list
    #         UpdateExpression="SET versions = list_append(versions,:d)",
    #         ExpressionAttributeValues={":d": [dataset]},
    #         ReturnValues="UPDATED_NEW",
    #     )

    # except Exception as e:
    #     return {
    #         "statusCode": 500,
    #         "headers": {
    #             "Access-Control-Allow-Origin": CORS_ALLOW,
    #         },
    #         "body": json.dumps({"message": str(e)}),
    #     }  # This should be logged

    # return {
    #     "statusCode": 200,
    #     "headers": {
    #         "Access-Control-Allow-Origin": CORS_ALLOW,
    #     },
    #     "body": json.dumps({"key": key}),
    # }
