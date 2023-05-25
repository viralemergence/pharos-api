import re
from datetime import datetime
from typing import Optional
import boto3
from pydantic import BaseModel, Extra, Field, ValidationError

# TODO remove
import pprint

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session
from column_alias import API_NAME_TO_UI_NAME_MAP
from engine import get_engine

from format import format_response
from models import PublishedRecord, PublishedDataset, PublishedProject, Researcher
from register import COMPLEX_FIELDS


SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


class Filter(BaseModel):
    field: str
    values: list[str]


class Parameters(BaseModel):
    page: int = Field(1, ge=1, alias="page")
    page_size: int = Field(10, ge=1, le=100, alias="pageSize")
    pharos_id: Optional[str] = Field(
        None, alias="pharosId", filter=lambda value: PublishedRecord.pharos_id == value
    )
    project_id: Optional[str] = Field(
        None,
        alias="projectId",
        filter=lambda value: PublishedRecord.project_id == value,
    )
    collection_start_date: Optional[str] = Field(
        None,
        alias="collectionStartDate",
        # TODO: We don't validate the date yet. Maybe add the strptime expression as a parameter to and_?
        filter=lambda value: and_(
            PublishedRecord.collection_date.isnot(None),
            PublishedRecord.collection_date >= datetime.strptime(value, "%Y-%m-%d"),
        ),
    )
    collection_end_date: Optional[str] = Field(
        None,
        alias="collectionEndDate",
        filter=lambda value: and_(
            PublishedRecord.collection_date.isnot(None),
            PublishedRecord.collection_date <= datetime.strptime(value, "%Y-%m-%d"),
        ),
    )
    project_name: Optional[list[str]] = Field(
        None,
        alias="projectName",
        filter=lambda value: PublishedRecord.dataset.has(
            PublishedDataset.project.has(PublishedProject.name == value)
        ),
    )
    host_species: Optional[list[str]] = Field(
        None,
        alias="hostSpecies",
        filter=lambda value: PublishedRecord.host_species.ilike(value),
    )
    pathogen: Optional[list[str]] = Field(
        None, filter=lambda value: PublishedRecord.pathogen.ilike(value)
    )
    detection_target: Optional[list[str]] = Field(
        None,
        alias="detectionTarget",
        filter=lambda value: PublishedRecord.detection_target.ilike(value),
    )
    researcher: Optional[list[str]] = Field(
        None,
        filter=lambda value: PublishedRecord.dataset.has(
            PublishedDataset.project.has(
                PublishedProject.researchers.any(Researcher.name == value)
            )
        ),
    )
    detection_outcome: Optional[list[str]] = Field(
        None,
        alias="detectionOutcome",
        filter=lambda value: PublishedRecord.detection_outcome.ilike(value),
    )

    class Config:
        extra = Extra.ignore


class GetPublishedRecordsEvent(BaseModel):
    query_string_parameters: Parameters = Field(
        Parameters, alias="queryStringParameters"
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

        researchers = published_record.dataset.project.researchers

        response_dict["pharosID"] = published_record.pharos_id
        response_dict["rowNumber"] = row_number + offset
        response_dict["Project name"] = published_record.dataset.project.name

        response_dict["Authors"] = ", ".join(
            [researcher.name for researcher in researchers]
        )

        response_dict["Collection date"] = published_record.collection_date.isoformat()
        response_dict["Latitude"] = latitude
        response_dict["Longitude"] = longitude

        for api_name, ui_name in API_NAME_TO_UI_NAME_MAP.items():
            if api_name not in COMPLEX_FIELDS:
                response_dict[ui_name] = getattr(published_record, api_name, None)

        response_rows.append(response_dict)

    return response_rows


def lambda_handler(event, _):
    # Consolidate multi-value and single-value query string paramaters
    multivalue_fields = [
        field
        for field in Parameters.__annotations__
        if str(Parameters.__annotations__[field]) == "typing.Optional[list[str]]"
    ]
    multivalue_field_aliases = [
        Parameters.__fields__[field].alias for field in multivalue_fields
    ]
    multivalue_params = {
        key: value
        for key, value in event["multiValueQueryStringParameters"].items()
        if key in multivalue_field_aliases
    }
    event["queryStringParameters"].update(multivalue_params)

    try:
        validated = GetPublishedRecordsEvent.parse_obj(event)
    except ValidationError as e:
        return format_response(400, e.json(), preformatted=True)

    engine = get_engine()

    limit = validated.query_string_parameters.page_size
    offset = (validated.query_string_parameters.page - 1) * limit

    with Session(engine) as session:
        query = session.query(
            PublishedRecord,
            PublishedRecord.geom.ST_X(),
            PublishedRecord.geom.ST_Y(),
        )
        filters = []
        for fieldname, field in Parameters.__fields__.items():
            get_filter = field.field_info.extra.get("filter")
            if get_filter is None:
                continue
            value_or_values = getattr(validated.query_string_parameters, fieldname)
            if value_or_values:
                if isinstance(value_or_values, list):
                    filters_for_field = [get_filter(value) for value in value_or_values]
                    filters.append(or_(*filters_for_field))
                else:
                    filters.append(get_filter(value_or_values))

        query = query.filter(and_(*filters))

        # Try to retrieve an extra row, to see if there are more pages
        rows = query.limit(limit + 1).offset(offset).all()

        is_last_page = len(rows) <= limit
        rows = rows[:limit]

        response_rows = format_response_rows(rows, offset)

    return format_response(
        200,
        {
            "publishedRecords": response_rows,
            "isLastPage": is_last_page,
        },
    )
