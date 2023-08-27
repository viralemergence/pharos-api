import boto3
from pydantic import BaseModel, Extra, Field, ValidationError

from sqlalchemy.orm import Session

from engine import get_engine
from format import format_response
from models import PublishedRecord

from published_records import (
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
        # Get the total number of records in the database
        record_count = session.query(PublishedRecord).count()

        # Get records that match the filters
        query = query_records(session, params)

        # Retrieve total number of matching records before limiting results to just one page
        matching_record_count = query.count()

        # Limit results to just one page
        query = query.limit(limit).offset(offset)

        # Execute the query
        rows = query.all()

    is_last_page = matching_record_count <= limit + offset

    response_rows = format_response_rows(rows, offset)

    return format_response(
        200,
        {
            "publishedRecords": response_rows,
            "isLastPage": is_last_page,
            "recordCount": record_count,
            "matchingRecordCount": matching_record_count,
        },
    )
