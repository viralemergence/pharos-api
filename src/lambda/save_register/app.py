"""Lambda function to save a register to S3"""

import os
from typing import Dict

import boto3
from pydantic import BaseModel, Extra, Field, ValidationError

from auth import check_auth
from format import format_response
from register import Record


S3CLIENT = boto3.client("s3")
N_VERSIONS = os.environ["N_VERSIONS"]
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class SaveRegisterData(BaseModel):
    """Data model for the save register request"""

    project_id: str = Field(alias="projectID")
    dataset_id: str = Field(alias="datasetID")
    register_data: Dict[str, Record] = Field(alias="register")

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):
    try:
        user = check_auth(event)
    except ValidationError:
        return format_response(403, "Not Authorized")

    if not user:
        return format_response(403, "Not Authorized")

    if not user.project_ids:
        return format_response(404, "Researcher has no projects")

    # parse and validate event data
    try:
        validated = SaveRegisterData.parse_raw(event["body"])
    except ValidationError as e:
        print(e.json(indent=2))
        return format_response(400, e)

    if validated.project_id not in user.project_ids:
        return format_response(403, "Researcher does not have access to this project")

    try:
        # Dump the validated register to JSON
        register_json = validated.json(
            include={"register_data"}, by_alias=True, exclude_none=True
        )
        # Create a unique key by combining the datasetID and the register hash
        encoded_data = bytes(register_json.encode("utf-8"))

        key = f"{validated.dataset_id}/data.json"

        # Save new register object to S3 bucket
        S3CLIENT.put_object(Bucket=DATASETS_S3_BUCKET, Body=(encoded_data), Key=key)

        return format_response(200, register_json, preformatted=True)

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
