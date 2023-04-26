import boto3
from geoalchemy2 import func
from pydantic import BaseModel, Extra, Field, ValidationError

from devtools import debug
from sqlalchemy import select
from sqlalchemy.orm import Session
from column_alias import API_NAME_TO_UI_NAME_MAP
from engine import get_engine

from format import format_response
from models import PublishedRecord


SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


class QueryStringParameters(BaseModel):
    page: int = 1
    page_size: int = Field(10, ge=0, le=100, alias="pageSize")

    class Config:
        extra = Extra.forbid


class GetPublishedRecordsEvent(BaseModel):
    query_string_parameters: QueryStringParameters = Field(
        QueryStringParameters(page=1, pageSize=10), alias="queryStringParameters"
    )

    class Config:
        extra = Extra.ignore


def lambda_handler(event, _):
    try:
        validated = GetPublishedRecordsEvent.parse_obj(event)
    except ValidationError as e:
        return format_response(400, e.json(), preformatted=True)

    debug(validated)

    engine = get_engine()

    page = validated.query_string_parameters.page
    page_size = validated.query_string_parameters.page_size

    with Session(engine) as session:
        records = session.scalars(
            select(
                PublishedRecord,
                func.ST_X(PublishedRecord.location).label("longitude"),
                func.ST_Y(PublishedRecord.location).label("latitude"),
            )
            .limit(page_size)
            .offset(page_size * (page - 1))
        ).all()

    debug(records)

    response_rows = []
    for record in records:
        response_dict: dict[str, str] = {}
        for api_name, ui_name in API_NAME_TO_UI_NAME_MAP.items():
            response_dict[ui_name] = str(getattr(record, api_name, None))

        response_dict["Collection date"] = record.collection_date.isoformat() + "Z"

        response_rows.append(response_dict)

    return format_response(200, response_rows)
