from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Extra, Field, validator

from sqlalchemy import and_, or_
from sqlalchemy.orm import selectinload

from column_alias import API_NAME_TO_UI_NAME_MAP
from models import (
    PublishedRecord,
    PublishedDataset,
    PublishedProject,
    Researcher,
)
from register import COMPLEX_FIELDS


class QueryStringParameters(BaseModel):
    page: int = Field(ge=1, alias="page")
    page_size: int = Field(ge=1, le=100, alias="pageSize")

    # The following fields filter the set of published records. Each "filter
    # function" will be used as a parameter to SQLAlchemy's Query.filter()
    # method.

    pharos_id: Optional[list[str]] = Field(
        None, filter_function=lambda value: PublishedRecord.pharos_id == value
    )
    dataset_id: Optional[str] = Field(
        None,
        filter_function=lambda value: PublishedRecord.dataset_id == value,
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
            PublishedDataset.project.has(PublishedProject.name.ilike(value))
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
        """Ensure that collection_start_date and collection_end_date are either
        valid dates or None"""
        if value is not None:
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError as exc:
                raise ValueError("Invalid date format. Should be YYYY-MM-DD") from exc
        return value

    class Config:
        extra = Extra.forbid


def get_compound_filter(params):
    """Return a compound filter ('condition AND condition AND condition...')
    for the specified parameters.
    """
    filters = []
    for fieldname, field in QueryStringParameters.__fields__.items():
        filter_function = field.field_info.extra.get("filter_function")
        if filter_function is None:
            continue
        # This field will be associated either with a single value or, if it's
        # a multi-value field, a list of values
        list_or_string = getattr(params, fieldname, None)
        if list_or_string:
            if isinstance(list_or_string, list):
                values = list_or_string
                filters_for_field = [filter_function(value) for value in values]
                # Suppose the query string was:
                # "?pathogen=Influenza&pathogen=Hepatitis". Then
                # `filters_for_field` would be equivalent to the following
                # list:
                #    [
                #      PublishedRecord.pathogen.ilike('Influenza'),
                #      PublishedRecord.pathogen.ilike('Hepatitis')]
                #    ]
                filters.append(or_(*filters_for_field))
                # `or_` is used here because we want records that match at
                # least one of the values that the user specified for this
                # field
            else:
                value = list_or_string
                filters.append(filter_function(value))
    conjunction = and_(True, *filters)  # Using `True` to avoid a deprecation warning

    # Suppose that the query string was:
    # "?host_species=Wolf&host_species=Bear&pathogen=Influenza&pathogen=Hepatitis".
    # Then `conjunction` could be written out like this:
    #    and_([
    #      or_([
    #          PublishedRecord.host_species.ilike('Wolf'),
    #          PublishedRecord.host_species.ilike('Bear'),
    #      ]),
    #      or_([
    #          PublishedRecord.pathogen.ilike('Influenza'),
    #          PublishedRecord.pathogen.ilike('Hepatitis'),
    #      ]),
    #    ])
    # In plain English, this means: "The host species is Wolf or Bear, and the
    # pathogen is Influenza or Hepatitis".

    return conjunction


def query_records(session, params):

    query = (
        session.query(
            PublishedRecord,
            PublishedRecord.geom.ST_X(),
            PublishedRecord.geom.ST_Y(),
        )
        .options(
            # In the dataset table, eagerly load the id
            selectinload(PublishedRecord.dataset).load_only(
                PublishedDataset.dataset_id
            ),
            # In the project table, eagerly load the project name
            selectinload(PublishedRecord.dataset)
            .selectinload(PublishedDataset.project)
            .load_only(PublishedProject.name),
            # In the researcher table, eagerly load the researcher names
            selectinload(PublishedRecord.dataset)
            .selectinload(PublishedDataset.project)
            .selectinload(PublishedProject.researchers)
            .load_only(Researcher.name),
        )
        .where(get_compound_filter(params))
        .order_by(PublishedRecord.pharos_id)
    )

    return query


def get_multi_value_query_string_parameters(event):
    parameters_annotations = (
        # pylint: disable=no-member
        QueryStringParameters.__annotations__
    )
    # Some fields, such as project_id and collection_start_date, take a single
    # value. Others take multiple values. We can tell which fields take
    # multiple values based on their type. Single-value fields have type
    # `Optional[str]`. Multi-value fields have the type `Optional[list[str]]`.
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


def format_response_rows(rows, offset):
    """Format the rows returned from the database to change API
    names into display names and add query-relative row numbers."""

    response_rows = []
    for row_number, row in enumerate(rows, start=1):
        (
            published_record,
            longitude,
            latitude,
        ) = row

        response_dict = {}

        response_dict["pharosID"] = published_record.pharos_id
        response_dict["rowNumber"] = row_number + offset

        project = published_record.dataset.project
        response_dict["Project name"] = project.name
        response_dict["Author"] = ", ".join(
            [researcher.name for researcher in project.researchers]
        )

        response_dict["Collection date"] = published_record.collection_date.isoformat()
        response_dict["Latitude"] = latitude
        response_dict["Longitude"] = longitude

        for api_name, ui_name in API_NAME_TO_UI_NAME_MAP.items():
            if api_name not in COMPLEX_FIELDS and ui_name not in response_dict:
                response_dict[ui_name] = getattr(published_record, api_name, None)

        response_rows.append(response_dict)

    return response_rows
