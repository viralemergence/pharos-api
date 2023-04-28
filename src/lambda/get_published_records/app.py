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
    page: int = Field(1, ge=1, alias="page")
    page_size: int = Field(10, ge=1, le=100, alias="pageSize")

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


def format_response_rows(rows, offset):
    """Format the rows returned from the database to change API
    names into display names, add query-relative row numbers,
    and add latitude, longitude, and collection date columns.
    """

    response_rows = []
    for row_number, row in enumerate(rows, start=1):
        published_record, longitude, latitude = row

        response_dict = {}
        response_dict["pharosID"] = published_record.pharos_id
        response_dict["rowNumber"] = row_number + offset
        response_dict["Project name"] = published_record.dataset.project.name

        for api_name, ui_name in API_NAME_TO_UI_NAME_MAP.items():
            if api_name not in COMPLEX_FIELDS:
                response_dict[ui_name] = getattr(published_record, api_name, None)

        response_dict["Latitude"] = latitude
        response_dict["Longitude"] = longitude
        response_dict["Collection date"] = published_record.collection_date.isoformat()

        response_rows.append(response_dict)

    return response_rows


def lambda_handler(event, _):
    try:
        validated = GetPublishedRecordsEvent.parse_obj(event)
    except ValidationError as e:
        return format_response(400, e.json(), preformatted=True)

    engine = get_engine()

    limit = validated.query_string_parameters.page_size
    offset = (validated.query_string_parameters.page - 1) * limit

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

        response_rows = format_response_rows(rows, offset)

    return format_response(200, {"publishedRecords": response_rows})
