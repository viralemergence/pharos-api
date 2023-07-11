import boto3
from pydantic import BaseModel, Extra, Field, ValidationError

from engine import get_engine
from format import format_response

from published_records import (
    format_response_rows,
    get_query,
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

    params = validated.query_string_parameters
    limit = params.page_size
    offset = (params.page - 1) * limit

    query = get_query(engine, params)

    # Try to retrieve an extra row, to see if there are more pages
    rows = query.limit(limit + 1).offset(offset).all()
    is_last_page = len(rows) <= limit
    # Don't include the extra row in the results
    rows = rows[:limit]

    response_rows = format_response_rows(rows, offset)

    return format_response(
        200,
        {
            "publishedRecords": response_rows,
            "isLastPage": is_last_page,
        },
    )
