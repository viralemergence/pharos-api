from datetime import datetime
from typing import Optional
import boto3
from pydantic import BaseModel, Extra, Field, ValidationError, validator

from sqlalchemy import and_, or_
from sqlalchemy.orm import Session
from column_alias import API_NAME_TO_UI_NAME_MAP
from engine import get_engine

from format import format_response
from models import PublishedRecord, PublishedDataset, PublishedProject, Researcher
from register import COMPLEX_FIELDS


SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


class Parameters(BaseModel):
    page: int = Field(1, ge=1, alias="page")
    page_size: int = Field(10, ge=1, le=100, alias="pageSize")
    pharos_id: Optional[str] = Field(
        None, filter=lambda value: PublishedRecord.pharos_id == value
    )
    project_id: Optional[str] = Field(
        None,
        filter=lambda value: PublishedRecord.dataset.project_id == value,
    )
    collection_start_date: Optional[str] = Field(
        None,
        filter=lambda value: and_(
            PublishedRecord.collection_date.isnot(None),
            PublishedRecord.collection_date >= datetime.strptime(value, "%Y-%m-%d"),
        ),
    )
    collection_end_date: Optional[str] = Field(
        None,
        filter=lambda value: and_(
            PublishedRecord.collection_date.isnot(None),
            PublishedRecord.collection_date <= datetime.strptime(value, "%Y-%m-%d"),
        ),
    )
    project_name: Optional[list[str]] = Field(
        None,
        filter=lambda value: PublishedRecord.dataset.has(
            PublishedDataset.project.has(PublishedProject.name == value)
        ),
    )
    host_species: Optional[list[str]] = Field(
        None,
        filter=PublishedRecord.host_species.ilike,
    )
    pathogen: Optional[list[str]] = Field(None, filter=PublishedRecord.pathogen.ilike)
    detection_target: Optional[list[str]] = Field(
        None, filter=PublishedRecord.detection_target.ilike
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
        None, filter=PublishedRecord.detection_outcome.ilike
    )

    @validator("collection_start_date", "collection_end_date", pre=True, always=True)
    def validate_date(cls, value):
        if value is not None:
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError as exc:
                raise ValueError("Invalid date format. Should be YYYY-MM-DD") from exc
        return value

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


def get_multi_value_query_string_parameters(event):
    parameters_annotations = Parameters.__annotations__  # pylint: disable=no-member
    multivalue_fields = [
        field
        for field in parameters_annotations
        if str(parameters_annotations[field]) == "typing.Optional[list[str]]"
    ]
    multivalue_field_aliases = [
        Parameters.__fields__[field].alias for field in multivalue_fields
    ]
    multivalue_params = {
        key: value
        for key, value in event["multiValueQueryStringParameters"].items()
        if key in multivalue_field_aliases
    }
    return multivalue_params


def get_compound_filter(params):
    """Return a compound filter, i.e. a filter of the form 'condition AND
    condition AND...', for the specified parameters.
    """
    filters = []
    for fieldname, field in Parameters.__fields__.items():
        get_filter = field.field_info.extra.get("filter")
        if get_filter is None:
            continue
        value_or_values = getattr(params, fieldname)
        if value_or_values:
            if isinstance(value_or_values, list):
                filters_for_field = [get_filter(value) for value in value_or_values]
                filters.append(or_(*filters_for_field))
            else:
                filters.append(get_filter(value_or_values))
    conjunction = and_(*filters)
    return conjunction


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
        query = session.query(
            PublishedRecord,
            PublishedRecord.geom.ST_X(),
            PublishedRecord.geom.ST_Y(),
        )
        query = query.filter(get_compound_filter(params))

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
