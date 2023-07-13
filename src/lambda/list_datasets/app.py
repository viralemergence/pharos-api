import os
import boto3
from boto3.dynamodb.conditions import Key
from botocore.utils import ClientError
from pydantic import BaseModel, Extra, Field, ValidationError

from auth import check_auth
from format import format_response
from register import Dataset


DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


class ListDatasetsBody(BaseModel):
    """Event data payload to list datasets."""

    researcher_id: str = Field(..., alias="researcherID")
    project_id: str = Field(..., alias="projectID")

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

    user = check_auth(validated.researcher_id)
    if not user:
        return format_response(403, "Not Authorized")
    if not user.project_ids:
        return format_response(403, "Researcher has no projects")
    if validated.project_id not in user.project_ids:
        return format_response(403, "Researcher does not have access to this project")

    try:
        datasets_response = METADATA_TABLE.query(
            KeyConditionExpression=Key("pk").eq(validated.project_id)
            & Key("sk").begins_with("set")
        )

        datasets: dict[str, dict] = {}

        for item in datasets_response["Items"]:
            dataset = Dataset.parse_table_item(item)
            datasets[dataset.dataset_id] = dataset.dict(by_alias=True)

        return format_response(200, datasets)

    except ClientError as e:
        return format_response(403, e)
