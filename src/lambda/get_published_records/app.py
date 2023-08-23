import boto3
from pydantic import BaseModel, Extra, Field, ValidationError

from sqlalchemy.orm import Session

from engine import get_engine
from format import format_response

from published_records import (
    format_response_rows,
    query_records,
    get_multi_value_query_string_parameters,
    QueryStringParameters,
    COLUMN_ID_TO_SORT_FIELD,
)
from models import PublishedRecord
from column_alias import UI_NAME_TO_API_NAME_MAP

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


class GetPublishedRecordsEvent(BaseModel):
    query_string_parameters: QueryStringParameters = Field(
        QueryStringParameters, alias="queryStringParameters"
    )

    class Config:
        extra = Extra.ignore


class FieldDoesNotExistException(Exception):
    pass


def lambda_handler(event, _):
    multivalue_params = get_multi_value_query_string_parameters(event)
    event["queryStringParameters"].update(multivalue_params)

    try:
        validated = GetPublishedRecordsEvent.parse_obj(event)
    except ValidationError as e:
        return format_response(400, e.json(), preformatted=True)

    engine = get_engine()

    params = validated.query_string_parameters
    print("params")
    print(params)
    limit = params.page_size
    offset = (params.page - 1) * limit

    with Session(engine) as session:
        query = query_records(session, params)

        if params.sort:
            for sort in params.sort:
                order = "asc"
                if sort.startswith("-"):
                    order = "desc"
                    sort = sort[1:]
                field_to_sort = COLUMN_ID_TO_SORT_FIELD[UI_NAME_TO_API_NAME_MAP[sort]]
                if not field_to_sort:
                    raise FieldDoesNotExistException
                print("field_to_sort")
                print(field_to_sort)

                if order == "desc":
                    field_to_sort = field_to_sort.desc()
                query = query.order_by(field_to_sort)

        query = query.order_by(PublishedRecord.pharos_id)

        print(str(query.statement))

        # Try to retrieve an extra row, to see if there are more pages
        query = query.limit(limit + 1).offset(offset)

        rows = query.all()  # execute the query

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
