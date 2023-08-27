import boto3
from pydantic import BaseModel, Extra, Field, ValidationError

from sqlalchemy.orm import Session

from engine import get_engine
from format import format_response
from models import PublishedRecord

from published_records import (
    get_published_records_response,
    get_multi_value_query_string_parameters,
    QueryStringParameters,
)

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


class GetPublishedRecordsEvent(BaseModel):
    query_string_parameters: QueryStringParameters = Field(
        QueryStringParameters, alias="queryStringParameters"
    )

    class Config:
        extra = Extra.ignore


def lambda_handler(event, _):
    multivalue_params = get_multi_value_query_string_parameters(event)
    event["queryStringParameters"].update(multivalue_params)

    try:
        validated = GetPublishedRecordsEvent.parse_obj(event)
    except ValidationError as e:
        return format_response(400, e.json(), preformatted=True)

    engine = get_engine()
    response = get_published_records_response(engine, validated.query_string_parameters)

    return format_response(200, response)
