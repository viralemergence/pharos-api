import hashlib
import json
import os

import boto3

DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]
N_VERSIONS = os.environ["N_VERSIONS"]

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]

USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


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
        key = f'{post_data["datasetID"]}/{md5hash}.json'

        save = S3CLIENT.put_object(
            Bucket=DATASETS_S3_BUCKET,
            Body=(data_string),
            Key=key,
        )

    except Exception as e:  # pylint: disable=broad-except
        return {
            "statusCode": 403,
            "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
            "body": json.dumps({"exception": str(e)}),
        }

    try:
        # Verify the new register has been saved
        S3CLIENT.head_object(Bucket=DATASETS_S3_BUCKET, Key=key)
    except Exception as e:  # pylint: disable=broad-except
        return {
            "statusCode": 403,
            "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
            "body": json.dumps({"exception": str(e)}),
        }

    try:

        # Check the number of files inside the folder
        dataset_list = S3CLIENT.list_objects_v2(
            Bucket=DATASETS_S3_BUCKET, Prefix=f'{post_data["datasetID"]}/'
        )

        length = len(dataset_list["Contents"])

        # Delete the oldest element of the list if greater than n_versions
        if length > int(N_VERSIONS):
            dataset_list["Contents"].sort(key=lambda item: item["LastModified"])
            delkey = dataset_list["Contents"][0]["Key"]
            response = S3CLIENT.delete_object(Bucket=DATASETS_S3_BUCKET, Key=delkey)

        return {
            # sending the status code on so that the
            # response actually reflects the success of saving
            "statusCode": save["ResponseMetadata"]["HTTPStatusCode"],
            "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
            "body": json.dumps(post_data["data"]),
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
