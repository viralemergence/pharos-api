import hashlib
import json
import os
import boto3
from auth import check_auth
from format import format_response


# This function should save each row of the register and the dataset
DYNAMODB = boto3.resource("dynamodb")
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])


def split(register: list, bin_size=25) -> list:
    register_length = len(register)
    for i in range(register_length):
        yield register[i:bin_size]


def lambda_handler(event, _):
    post_data = json.loads(event.get("body", "{}"))

    # Placeholder check user authorization
    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:

        register = post_data["register"]

        # Need to coerce register to list
        register_list = list(register.values())

        # When code exits with block, batch writer will send the data to DynamoDB.
        # Only allows 25 put_items
        bins = split(register_list)

        for bin_ in bins:
            with DATASETS_TABLE.batch_write() as batch:
                for record in bin_:
                    batch.put_item(Item=record)

        return format_response(200, post_data["data"])

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
