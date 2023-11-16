from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from column_alias import API_NAME_TO_UI_NAME_MAP, UI_NAME_TO_API_NAME_MAP
from models import PublishedDataset, PublishedProject, PublishedRecord, Researcher
from published_records_metadata import sortable_fields
from pydantic import BaseModel, Extra, Field, validator
from register import COMPLEX_FIELDS
from sqlalchemy import and_, or_
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Query, Session, selectinload


class FieldDoesNotExistException(Exception):
    pass


class FiltersQueryStringParameters(BaseModel):
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
    researcher_name: Optional[list[str]] = Field(
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


class QueryStringParameters(FiltersQueryStringParameters):
    page: int = Field(ge=1, alias="page")
    page_size: int = Field(ge=1, le=100, alias="pageSize")
    sort: Optional[list[str]] = Field(alias="sort")

    class Config:
        extra = Extra.forbid


def get_compound_filter(params: QueryStringParameters):
    """Create a compound filter ('condition AND condition AND condition...')
    for the specified parameters. This function returns a tuple:
    (compound_filter, filter_count)
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

    # Suppose that the query string is:
    # "?host_species=Wolf&host_species=Bear&pathogen=Influenza&pathogen=Hepatitis".
    # Then `conjunction` is equivalent to:
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
    # pathogen is Influenza or Hepatitis."

    return (conjunction, len(filters))


def query_records(session: Session, params: QueryStringParameters) -> Tuple[Query, int]:
    """Returns a tuple: (query, filter_count)"""
    (compound_filter, filter_count) = get_compound_filter(params)
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
        .where(compound_filter)
    )

    return (query, filter_count)


def get_multi_value_query_string_parameters(event):
    parameters_annotations = (
        # pylint: disable=no-member
        QueryStringParameters.__annotations__ | FiltersQueryStringParameters.__annotations__

    )

    print("parameters_annotations")
    print(parameters_annotations)

    # Some fields, such as project_id and collection_start_date, take a single
    # value. Others take multiple values. We can tell which fields take
    # multiple values based on their type. Single-value fields have type
    # `Optional[str]`. Multi-value fields have the type `Optional[list[str]]`.
    multivalue_fields = [
        field
        for field in parameters_annotations
        if parameters_annotations.get(field) and str(parameters_annotations[field]) == "typing.Optional[list[str]]"
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

        response_dict["Project"] = project.name
        response_dict["Researcher"] = [
            {
                "name": researcher.name,
                "researcherID": researcher.researcher_id,
            }
            for researcher in project.researchers
        ]
        response_dict["Collection date"] = published_record.collection_date.isoformat()
        response_dict["Latitude"] = latitude
        response_dict["Longitude"] = longitude

        for api_name, ui_name in API_NAME_TO_UI_NAME_MAP.items():
            if api_name not in COMPLEX_FIELDS and ui_name not in response_dict:
                response_dict[ui_name] = getattr(published_record, api_name, None)

        response_rows.append(response_dict)

    return response_rows


def get_published_records_response(
    engine: Engine, params: QueryStringParameters
) -> Dict[str, Any]:
    limit = params.page_size
    offset = (params.page - 1) * limit

    with Session(engine) as session:
        # Get the total number of records in the database
        record_count = session.query(PublishedRecord).count()

        # Get records that match the filters
        (query, _) = query_records(session, params)

        # Retrieve total number of matching records before limiting results to just one page
        matching_record_count = query.count()

        if params.sort:
            for sort in params.sort:
                order = "asc"
                if sort.startswith("-"):
                    order = "desc"
                    sort = sort[1:]
                field_to_sort = sortable_fields.get(
                    UI_NAME_TO_API_NAME_MAP.get(sort) or ""
                )
                if not field_to_sort:
                    raise FieldDoesNotExistException
                print("field_to_sort")
                print(field_to_sort)

                if order == "desc":
                    field_to_sort = field_to_sort.desc()
                query = query.order_by(field_to_sort)

        query = query.order_by(PublishedRecord.pharos_id)

        # Limit results to just one page
        query = query.limit(limit).offset(offset)

        # Execute the query
        rows = query.all()

    is_last_page = matching_record_count <= limit + offset

    response_rows = format_response_rows(rows, offset)

    return {
        "publishedRecords": response_rows,
        "isLastPage": is_last_page,
        "recordCount": record_count,
        "matchingRecordCount": matching_record_count,
    }
