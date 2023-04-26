from typing import Optional
import boto3
from pydantic import BaseModel, Extra, Field, ValidationError

from sqlalchemy.orm import Session
from column_alias import API_NAME_TO_UI_NAME_MAP
from engine import get_engine

from format import format_response
from models import PublishedRecord
from register import COMPLEX_FIELDS


SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


class Filter(BaseModel):
    field: str
    values: list[str]


class QueryStringParameters(BaseModel):
    page: int = 1
    page_size: int = Field(10, ge=0, le=100, alias="pageSize")

    # researcher: Optional[str]
    # host_species: Optional[str] = Field(None, alias="hostSpecies")
    # pathogen: Optional[str]
    # detection_target: Optional[str] = Field(None, alias="detectionTarget")
    # detection_outcome: Optional[str] = Field(None, alias="detectionOutcome")

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

    engine = get_engine()

    limit = validated.query_string_parameters.page_size
    offset = validated.query_string_parameters.page * (limit - 1)

    with Session(engine) as session:
        rows = (
            session.query(
                PublishedRecord,
                PublishedRecord.location.ST_X(),
                PublishedRecord.location.ST_Y(),
            )
            .limit(limit)
            .offset(offset)
            .all()
        )

    response_rows = []
    for published_record, longitude, latitude in rows:

        response_dict = {}
        for api_name, ui_name in API_NAME_TO_UI_NAME_MAP.items():
            if api_name not in COMPLEX_FIELDS:
                response_dict[ui_name] = getattr(published_record, api_name, None)

        response_dict["Latitude"] = latitude
        response_dict["Longitude"] = longitude
        response_dict["Collection date"] = published_record.collection_date.isoformat()

        response_rows.append(response_dict)

    return format_response(200, response_rows)
