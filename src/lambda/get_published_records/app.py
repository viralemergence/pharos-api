import re
from datetime import datetime
from typing import Optional
import boto3
from pydantic import BaseModel, Extra, Field, ValidationError


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


class QueryStringParameters(BaseModel):
    page: int = Field(1, ge=1, alias="page")
    page_size: int = Field(10, ge=1, le=100, alias="pageSize")
    pharos_id: Optional[str] = Field(None, alias="pharosId")
    project_id: Optional[str] = Field(None, alias="projectId")
    project_name: Optional[str] = Field(None, alias="projectName")
    host_species: Optional[str] = Field(None, alias="hostSpecies")
    pathogen: Optional[str]
    detection_target: Optional[str] = Field(None, alias="detectionTarget")
    researcher: Optional[str]
    detection_outcome: Optional[str] = Field(None, alias="detectionOutcome")
    collection_start_date: Optional[str] = Field(None, alias="collectionStartDate")
    collection_end_date: Optional[str] = Field(None, alias="collectionEndDate")

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


def split_on_comma(value):
    return re.split(r"\s*,\s*", value)


def lambda_handler(event, _):
    try:
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
            for fieldname in [
                "host_species",
                "pathogen",
                "detection_target",
                "detection_outcome",
            ]:
                filter_value_or_values = getattr(
                    validated.query_string_parameters, fieldname
                )
                if filter_value_or_values:
                    values = split_on_comma(filter_value_or_values)
                    filters_for_field = []
                    for value in values:
                        values = value.strip()
                        filters_for_field.append(
                            getattr(PublishedRecord, fieldname).ilike(value)
                        )
                    filters.append(or_(*filters_for_field))

            collection_start_date_str = (
                validated.query_string_parameters.collection_start_date
            )
            if collection_start_date_str:
                collection_start_date = datetime.strptime(
                    collection_start_date_str, "%Y-%m-%d"
                )
                if collection_start_date:
                    filters.append(
                        and_(
                            PublishedRecord.collection_date.isnot(None),
                            PublishedRecord.collection_date >= collection_start_date,
                        )
                    )

            collection_end_date_str = (
                validated.query_string_parameters.collection_end_date
            )
            if collection_end_date_str:
                collection_end_date = datetime.strptime(
                    collection_end_date_str, "%Y-%m-%d"
                )
                if collection_end_date:
                    filters.append(
                        and_(
                            PublishedRecord.collection_date.isnot(None),
                            PublishedRecord.collection_date <= collection_end_date,
                        )
                    )

            pharos_id = validated.query_string_parameters.pharos_id
            if pharos_id:
                filters.append(PublishedRecord.pharos_id == pharos_id)

            project_name = validated.query_string_parameters.project_name
            if project_name:
                filters.append(
                    PublishedRecord.dataset.has(
                        PublishedDataset.project.has(
                            PublishedProject.name == project_name
                        )
                    )
                )

            researchers = validated.query_string_parameters.researcher
            if researchers:
                filters_for_researchers = []
                for researcher in split_on_comma(researchers):
                    filters_for_researchers.append(
                        PublishedRecord.dataset.has(
                            PublishedDataset.project.has(
                                PublishedProject.researchers.any(
                                    Researcher.name == researcher
                                )
                            )
                        )
                    )
                filters.append(or_(*filters_for_researchers))

            project_id = validated.query_string_parameters.project_id
            if project_id:
                filters.append(
                    PublishedRecord.dataset.has(
                        PublishedDataset.project_id == project_id
                    )
                )

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

    except Exception as e:
        return format_response(500, {"error": str(e)})
