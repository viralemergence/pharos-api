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


class QueryStringParameters(BaseModel):
    page: int = Field(1, ge=1, alias="page")
    page_size: int = Field(10, ge=1, le=100, alias="pageSize")

    # The following fields filter the set of published records. The "filter
    # function" is a python function that will be used as a parameter to
    # SQLAlchemy's Query.filter() method.

    pharos_id: Optional[list[str]] = Field(
        None, filter_function=lambda value: PublishedRecord.pharos_id == value
    )
    project_id: Optional[str] = Field(
        None,
        filter_function=lambda value: PublishedRecord.dataset.project_id == value,
    )
    collection_start_date: Optional[str] = Field(
        None,
        filter_function=lambda value: and_(
            PublishedRecord.collection_date.isnot(None),
            PublishedRecord.collection_date >= datetime.strptime(value, "%Y-%m-%d"),
        ),
    )
    collection_end_date: Optional[str] = Field(
        None,
        filter_function=lambda value: and_(
            PublishedRecord.collection_date.isnot(None),
            PublishedRecord.collection_date <= datetime.strptime(value, "%Y-%m-%d"),
        ),
    )
    project_name: Optional[list[str]] = Field(
        None,
        filter_function=lambda value: PublishedRecord.dataset.has(
            PublishedDataset.project.has(PublishedProject.name == value)
        ),
    )
    host_species: Optional[list[str]] = Field(
        None,
        filter_function=PublishedRecord.host_species.ilike,
    )
    pathogen: Optional[list[str]] = Field(
        None, filter_function=PublishedRecord.pathogen.ilike
    )
    detection_target: Optional[list[str]] = Field(
        None, filter_function=PublishedRecord.detection_target.ilike
    )
    researcher: Optional[list[str]] = Field(
        None,
        filter_function=lambda value: PublishedRecord.dataset.has(
            PublishedDataset.project.has(
                PublishedProject.researchers.any(Researcher.name == value)
            )
        ),
    )
    detection_outcome: Optional[list[str]] = Field(
        None, filter_function=PublishedRecord.detection_outcome.ilike
    )

    @validator("collection_start_date", "collection_end_date", pre=True, always=True)
    def validate_date(cls, value):
        """Ensure that collection_start_date and collection_end_date are either valid dates or None"""
        if value is not None:
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError as exc:
                raise ValueError("Invalid date format. Should be YYYY-MM-DD") from exc
        return value

    class Config:
        extra = Extra.forbid


class GetPublishedRecordsEvent(BaseModel):
    query_string_parameters: QueryStringParameters = Field(
        QueryStringParameters, alias="queryStringParameters"
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
    parameters_annotations = (
        QueryStringParameters.__annotations__
    )  # pylint: disable=no-member
    # Some fields, such as project_id and collection_start_date, take a single
    # value. Others take multiple values. We can tell which fields take
    # multiple values by their type. Single-value fields have type
    # `Optional[str]`, while multi-value fields have the type
    # `Optional[list[str]]`.
    multivalue_fields = [
        field
        for field in parameters_annotations
        if str(parameters_annotations[field]) == "typing.Optional[list[str]]"
    ]
    multivalue_field_aliases = [
        QueryStringParameters.__fields__[field].alias for field in multivalue_fields
    ]
    multivalue_params = {
        key: value
        for key, value in event["multiValueQueryStringParameters"].items()
        if key in multivalue_field_aliases
    }
    return multivalue_params


def get_compound_filter(params):
    """Return a compound filter --- a filter of the form 'condition AND
    condition AND condition [etc.]' --- for the specified parameters.
    """
    filters = []
    for fieldname, field in QueryStringParameters.__fields__.items():
        filter_function = field.field_info.extra.get("filter_function")
        if filter_function is None:
            continue
        # This field will be associated either with a single value or, if it's
        # a multi-value field, with a list of values
        list_or_string = getattr(params, fieldname)
        if list_or_string:
            if isinstance(list_or_string, list):
                values = list_or_string
                filters_for_field = [filter_function(value) for value in values]
                # Suppose the query string was:
                # "?pathogen=Ebola&pathogen=Hepatitis". Then
                # `filters_for_field` would be equivalent to the following
                # Python list:
                #    [
                #      PublishedRecord.pathogen.ilike('Ebola'),
                #      PublishedRecord.pathogen.ilike('Hepatitis')]
                #    ]
                filters.append(or_(*filters_for_field))
                # `or_` is used here because we want records that match at
                # least one of the values that the user specified for this
                # field
            else:
                value = list_or_string
                filters.append(filter_function(value))
    conjunction = and_(*filters)

    # Suppose that the query string was:
    # "?host_species=Wolf&host_species=Bear&pathogen=Ebola&pathogen=Hepatitis".
    # Then `conjunction` would be equivalent to:
    #    and_([
    #      or_([
    #          PublishedRecord.host_species.ilike('Wolf'),
    #          PublishedRecord.host_species.ilike('Bear'),
    #      ]),
    #      or_([
    #          PublishedRecord.pathogen.ilike('Ebola'),
    #          PublishedRecord.pathogen.ilike('Hepatitis'),
    #      ]),
    #    ])
    # In plain English, this means: "the host species is wolf or bear, and the
    # pathogen is ebola or hepatitis".

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
