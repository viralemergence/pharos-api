import boto3
from pydantic import BaseModel, Extra, Field, ValidationError

from sqlalchemy.orm import Session

from engine import get_engine
from format import format_response

from published_records import (
    add_related_data,
    format_response_rows,
    query_records,
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

    with Session(engine) as session:
        query = query_records(session, params)

        # Try to retrieve an extra row, to see if there are more pages
        query = query.limit(limit + 1).offset(offset)

        rows = query.all()  # execute the query

        rows = add_related_data(rows)

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
