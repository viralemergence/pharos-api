import os
import hashlib
from typing import Dict

import boto3
from pydantic import BaseModel, Field, ValidationError

from auth import check_auth
from format import format_response
from register import Record


S3CLIENT = boto3.client("s3")
N_VERSIONS = os.environ["N_VERSIONS"]
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class SaveRegisterData(BaseModel):
    researcherID: str
    datasetID: str
    register_data: Dict[str, Record] = Field(..., alias="register")


class Event(BaseModel):
    body: SaveRegisterData


def lambda_handler(event, _):

    # parse and validate event data
    try:
        validated = SaveRegisterData.parse_raw(event["body"])
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    # Placeholder check user authorization
    authorized = check_auth(validated.researcherID)
    if not authorized:
        return format_response(403, "Not Authorized")

    try:
        # Dump the validated register to JSON
        register_json = validated.json(
            include={"register_data"}, by_alias=True, exclude_none=True
        )
        # Create a unique key by combining the datasetID and the register hash
        encoded_data = bytes(register_json.encode("utf-8"))
        md5hash = str(hashlib.md5(encoded_data).hexdigest())
        key = f"{validated.datasetID}/{md5hash}.json"

        # Save new register object to S3 bucket
        S3CLIENT.put_object(Bucket=DATASETS_S3_BUCKET, Body=(encoded_data), Key=key)

        # # Check the number of files inside the folder
        dataset_list = S3CLIENT.list_objects_v2(
            Bucket=DATASETS_S3_BUCKET, Prefix=f"{validated.datasetID}/"
        )

        length = len(dataset_list["Contents"])

        # Delete the oldest element of the list if greater than n_versions
        if length > int(N_VERSIONS):
            dataset_list["Contents"].sort(key=lambda item: item["LastModified"])
            delkey = dataset_list["Contents"][0]["Key"]
            S3CLIENT.delete_object(Bucket=DATASETS_S3_BUCKET, Key=delkey)

        return format_response(200, register_json, preformatted=True)

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
