import base64
from typing import Optional

from engine import get_engine
from format import format_response
from models import PublishedDataset, PublishedProject, PublishedRecord
from published_records import (
    FiltersQueryStringParameters,
    get_compound_filter,
    get_multi_value_query_string_parameters,
)
from pydantic import BaseModel, Field, ValidationError, validator
from sqlalchemy import ColumnElement, select
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from urllib3.filepost import os

CORS_ALLOW = os.environ["CORS_ALLOW"]


class PathParameters(BaseModel):

    tile_path: str = Field(alias="tilePath")

    @validator("tile_path")
    def check_tile_path(cls, tile_path: str):
        try:
            coords, extension = tile_path.split(".")
            z, x, y = [int(chunk) for chunk in coords.split("/")]

        except ValueError as exc:
            raise ValueError(
                "Incorrect format; Map tile requests "
                "must be in the format /map/z/x/y.pbf"
            ) from exc

        if extension != "pbf":
            raise ValueError(
                f"Incorrect extension '.{extension}'; Map tile "
                "requests must be in the format /map/z/x/y.pbf"
            )

        min_z, max_z = 0, 14
        if not min_z <= int(z) <= max_z:
            raise ValueError(
                "Unsupported zoom level; zoom level "
                f"must be between {min_z} and {max_z}"
            )

        if not 0 <= y <= 2**z - 1:
            raise ValueError(
                f"y value {y} is out of bounds, for zoom "
                f"level {z}, y must be >0 and <{2**z-1}."
            )

        if not 0 <= x <= 2**z - 1:
            raise ValueError(
                f"x value {x} is out of bounds, for zoom "
                f"level {z}, y must be >0 and <{2**z-1}."
            )

        return tile_path

    @property
    def z(self):
        return int(self.tile_path.split("/")[0])

    @property
    def x(self):
        return int(self.tile_path.split("/")[1])

    @property
    def y(self):
        return int(self.tile_path.split("/")[2].split(".")[0])


class GetMapTileEvent(BaseModel):
    """Event payload for requesting a pharos map tile"""

    path_parameters: PathParameters = Field(alias="pathParameters")
    query_string_parameters: Optional[FiltersQueryStringParameters] = Field(
        alias="queryStringParameters",
    )


def lambda_handler(event, _):

    multivalue_params = get_multi_value_query_string_parameters(event)
    if multivalue_params:
        event["queryStringParameters"].update(multivalue_params)

    try:
        validated = GetMapTileEvent.parse_obj(event)
    except ValidationError as e:
        return format_response(400, e.json(), preformatted=True)

    engine = get_engine()

    with Session(engine) as session:

        (compound_filter, _) = get_compound_filter(validated.query_string_parameters)

        layer_name = "pharos-points"
        mvt_geom = (
            select(
                func.ST_AsMVTGeom(
                    func.ST_Transform(PublishedRecord.geom, 3857),
                    func.ST_TileEnvelope(
                        validated.path_parameters.z,
                        validated.path_parameters.x,
                        validated.path_parameters.y,
                    ),
                ),
                PublishedRecord.pharos_id,
                PublishedProject.name.label("project_name"),
            )
            .select_from(PublishedRecord)
            .join(PublishedDataset)
            .join(PublishedProject)
            .where(compound_filter)
            .cte("mvt_geom")
        )

        query = select(
            func.ST_AsMVT(mvt_geom.table_valued(), layer_name),
        ).select_from(mvt_geom)

        memory = session.scalar(query)

        if not memory:
            tile_bytes = bytes()
        else:
            tile_bytes = bytes(memory)

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
                "Content-Type": "application/octet-stream",
            },
            "body": base64.b64encode(tile_bytes).decode("utf-8"),
            "isBase64Encoded": True,
        }
