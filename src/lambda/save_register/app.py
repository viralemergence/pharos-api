import json
import os
import sys
import time
import boto3
from auth import check_auth
from format import format_response


# This function should save each row of the register and the dataset
DYNAMODB = boto3.resource("dynamodb")
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


# def split(register: list, bin_size=25) -> list:
#     register_length = len(register)
#     for i in range(register_length):
#         yield register[i:bin_size]


def lambda_handler(event, _):
    """
    When code exits with block, batch writer will send the data to DynamoDB.
    Batch write only allows 25 put_items operations or 16mb size uploads per batch.
    Upload is handled directly by batch_writer()
    """

    post_data = json.loads(event.get("body", "{}"))

    # Placeholder check user authorization
    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:

        register = post_data["register"]

        with DATASETS_TABLE.batch_writer() as batch:
            for record in list(register.items()):  # Iterate over tuples
                batch.put_item(
                    Item={
                        "datasetID": post_data["datasetID"],
                        "recordID": record[0],
                        "record": record[1],
                    }
                )

        return format_response(200, "Succesful upload")

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
