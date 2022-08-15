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
    """
    When code exits with block, batch writer will send the data to DynamoDB.
    Batch write only allows 25 put_items operations or 16mb size uploads per batch.
    TODO: manage upload size
    """
    post_data = json.loads(event.get("body", "{}"))

    # Placeholder check user authorization
    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:

        register = post_data["register"]

        # Coerce register to list of tuples [ (recordID, record), ... ]
        register_list = list(register.items())

        # Split register in bins of size <= 25.
        bins = split(register_list)  # List of lists

        for bin_ in bins:  # Iterate over lists
            with DATASETS_TABLE.batch_writer() as batch:
                for record in bin_:  # Iterate over tuples
                    batch.put_item(
                        Item={
                            "datasetID": post_data["datasetID"],
                            "recordID": record[0],
                            "record": record[1],
                        }
                    )

        # Update meta information
        meta = {}  # something

        DATASETS_TABLE.put_item(
            Item={
                "datasetID": post_data["datasetID"],
                "recordID": "_meta",
                "record": meta,
            }
        )

        return format_response(200, "Succesful upload")

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, str(e))
