import json
import os
import hashlib
import boto3
from auth import check_auth
from format import format_response
from validator.definitions import Record
from validator.validate_record import validate_record


N_VERSIONS = os.environ["N_VERSIONS"]
S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


def lambda_handler(event, _):
    post_data = json.loads(event.get("body", "{}"))

    # Placeholder check user authorization
    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:
        # Validate register
        verified_register = {}

        for record_id, record in post_data["register"].items():
            record_ = Record(record, record_id)
            record_ = validate_record(record_)
            verified_register[record_id] = record_.get_record()

        # Create a unique key by combining the datasetID and the register hash
        encoded_data = bytes(json.dumps(verified_register).encode("UTF-8"))
        md5hash = str(hashlib.md5(encoded_data).hexdigest())
        key = f'{post_data["datasetID"]}/{md5hash}.json'

        # Save new register object to S3 bucket
        S3CLIENT.put_object(Bucket=DATASETS_S3_BUCKET, Body=(encoded_data), Key=key)

        # # Check the number of files inside the folder
        dataset_list = S3CLIENT.list_objects_v2(
            Bucket=DATASETS_S3_BUCKET, Prefix=f'{post_data["datasetID"]}/'
        )

        length = len(dataset_list["Contents"])

        # Delete the oldest element of the list if greater than n_versions
        if length > int(N_VERSIONS):
            dataset_list["Contents"].sort(key=lambda item: item["LastModified"])
            delkey = dataset_list["Contents"][0]["Key"]
            S3CLIENT.delete_object(Bucket=DATASETS_S3_BUCKET, Key=delkey)

        return format_response(200, verified_register)

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
