from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Extra, Field, validator

from sqlalchemy import and_, or_, __version__ as SQLALCHEMY_VERSION
from sqlalchemy.orm import Session
from column_alias import API_NAME_TO_UI_NAME_MAP
from models import PublishedRecord, PublishedDataset, PublishedProject, Researcher
from register import COMPLEX_FIELDS

print("sqlalchemy version", SQLALCHEMY_VERSION)


class QueryStringParameters(BaseModel):
    page: int = Field(1, ge=1, alias="page")
    page_size: int = Field(10, ge=1, le=100, alias="pageSize")

    # The following fields filter the set of published records. Each "filter
    # function" will be used as a parameter to SQLAlchemy's Query.filter()
    # method.

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
    """Return a compound filter --- a filter of the form 'condition AND
    condition AND condition [etc.]' --- for the specified parameters.
    """
    filters = []
    for fieldname, field in QueryStringParameters.__fields__.items():
        filter_function = None

        # We have to look in two places for the field_info, because sometimes
        # `field` has a property, `field_info`, but at other times, `field` is
        # itself a FieldInfo object
        field_info = getattr(field, "field_info", None) or field
        extra = getattr(field_info, "extra", None)
        if extra:
            filter_function = extra.get("filter_function")
        if filter_function is None:
            continue
        # This field will be associated either with a single value or, if it's
        # a multi-value field, with a list of values
        list_or_string = getattr(params, fieldname, None)
        if list_or_string:
            if isinstance(list_or_string, list):
                values = list_or_string
                filters_for_field = [filter_function(value) for value in values]
                # Suppose the query string was:
                # "?pathogen=Influenza&pathogen=Hepatitis". Then
                # `filters_for_field` would be equivalent to the following
                # Python list:
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
    # Then `conjunction` would be equivalent to:
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
    # In plain English, this means: "the host species is wolf or bear, and the
    # pathogen is influenza or hepatitis".

    return conjunction


def get_query(engine, params):
    with Session(engine) as session:
        query = session.query(
            PublishedRecord,
            PublishedRecord.geom.ST_X(),
            PublishedRecord.geom.ST_Y(),
        )
        query = query.filter(get_compound_filter(params))
        return query


def get_multi_value_query_string_parameters(event):
    parameters_annotations = (
        # pylint: disable=no-member
        QueryStringParameters.__annotations__
    )
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
