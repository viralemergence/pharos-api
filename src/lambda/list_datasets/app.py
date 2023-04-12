import os
import boto3
from boto3.dynamodb.conditions import Key
from botocore.utils import ClientError
from pydantic import BaseModel, Extra, ValidationError

from auth import check_auth
from format import format_response


DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


class ListDatasetsBody(BaseModel):
    """Event data payload to list datasets."""

    researcherID: str
    projectID: str

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):
    """
    List datasets per project
    """

    try:
        validated = ListDatasetsBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    user = check_auth(validated.researcherID)
    if not user:
        return format_response(403, "Not Authorized")
    if not user.projectIDs:
        return format_response(403, "Researcher has no projects")
    if validated.projectID not in user.projectIDs:
        return format_response(403, "Researcher does not have access to this project")

    try:
        datasets_response = METADATA_TABLE.query(
            KeyConditionExpression=Key("pk").eq(validated.projectID)
            & Key("sk").begins_with("set")
        )

        datasets = {}

        # rename pk and sk the the dataset-specific names
        for dataset in datasets_response["Items"]:
            dataset["projectID"] = dataset.pop("pk")
            dataset["datasetID"] = dataset.pop("sk")

            datasets[dataset["datasetID"]] = dataset

        return format_response(200, datasets)

    except ClientError as e:
        return format_response(403, e)
