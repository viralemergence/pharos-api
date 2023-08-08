from typing import Optional

import boto3
from pydantic import BaseModel, Extra, Field, ValidationError

from engine import get_engine
from format import format_response
from researchers import get_researchers


SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


class QueryStringParameters(BaseModel):
    researcher_ids: list[str] = Field(alias="researcherID")

    class Config:
        extra = Extra.forbid


class GetResearchersEvent(BaseModel):
    multi_value_query_string_parameters: Optional[QueryStringParameters] = Field(
        alias="multiValueQueryStringParameters"
    )

    class Config:
        extra = Extra.ignore


def lambda_handler(event, _):

    try:
        validated = GetResearchersEvent.parse_obj(event)
    except ValidationError as e:
        return format_response(400, e.json(), preformatted=True)

    engine = get_engine()

    researcher_ids: list[str] = []
    if validated.multi_value_query_string_parameters:
        researcher_ids = validated.multi_value_query_string_parameters.researcher_ids

    try:
        researchers = get_researchers(engine, researcher_ids)
    except ValueError as e:
        return format_response(404, {"message": str(e)})

    return format_response(200, researchers)
