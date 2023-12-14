import json
import os
from typing import Dict

import boto3
from auth import check_auth
from botocore.exceptions import ClientError
from format import format_response
from pydantic import BaseModel, Extra, Field, ValidationError
from register import Record, Register

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class SaveRecordsData(BaseModel):
    """Data model for the save records request"""

    project_id: str = Field(alias="projectID")
    dataset_id: str = Field(alias="datasetID")
    records: Dict[str, Record]

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):
    try:
        user = check_auth(event)
    except ValidationError as e:
        return format_response(400, "Not Authorized")

    if not user:
        return format_response(400, "Not Authorized")

    if not user.project_ids:
        return format_response(400, "Not Authorized")

    try:
        validated = SaveRecordsData.parse_raw(event["body"])
    except ValidationError as e:
        print(e.json())
        return format_response(400, e.json())

    if validated.project_id not in user.project_ids:
        return format_response(400, "Researcher does not have access to this project")

    register_json = ""

    # Check for previous records
    try:
        key = f"{validated.dataset_id}/data.json"
        register_json = (
            S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)["Body"]
            .read()
            .decode("utf-8")
        )

    except ClientError as e:
        return format_response(500, e)

    previous = json.loads(register_json)
    previous_register = Register.construct(**previous)
    response_register = Register.parse_obj({"register": validated.records})

    for record_id, record in validated.records.items():
        if previous["register"].get(record_id):
            previous_record = Record(**previous["register"][record_id])
            merge_result = Record.merge(previous_record, record)

            if merge_result is None:
                return format_response(400, "Record merge failed")

            response_register.register_data[record_id] = merge_result
            previous_register.register_data[record_id] = merge_result

        else:
            previous_register.register_data[record_id] = validated.records[record_id]

    try:
        # Dump the modified register to JSON
        register_json = previous_register.json(by_alias=True, exclude_none=True)
        # Create a unique key by combining the datasetID and the register hash
        encoded_data = bytes(register_json.encode("utf-8"))

        key = f"{validated.dataset_id}/data.json"

        # Save new register object to S3 bucket
        S3CLIENT.put_object(Bucket=DATASETS_S3_BUCKET, Body=(encoded_data), Key=key)

        return format_response(
            200,
            response_register.json(by_alias=True, exclude_none=True),
            preformatted=True,
        )

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
