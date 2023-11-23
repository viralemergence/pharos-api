import base64
from dataclasses import dataclass
from sqlalchemy import select

from sqlalchemy.orm import Session

from sqlalchemy.sql import func
from urllib3.filepost import os
from engine import get_engine

from format import format_response
from models import PublishedDataset, PublishedProject, PublishedRecord


CORS_ALLOW = os.environ["CORS_ALLOW"]

@dataclass
class TilePath:
    """Validate and parse the path of a map tile request."""

    z: int
    x: int
    y: int

    @classmethod
    def validate(cls, path):
        try:
            coords, extension = path.split(".")
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

        min_z, max_z = 0, 20
        if not min_z <= int(z) <= max_z:
            raise ValueError(
                "Unsupported zoom level; zoom level "
                f"must be between {min_z} and {max_z}"
            )

        return cls(z, x, y)


def lambda_handler(event, _):
    try:
        tile_path = TilePath.validate(event["pathParameters"]["mapPath"])
    except ValueError as exc:
        return format_response(400, {"error": str(exc)})

    engine = get_engine()

    with Session(engine) as session:
        layer_name = "pharos-points"
        mvt_geom = (
            select(
                func.ST_AsMVTGeom(
                    func.ST_Transform(PublishedRecord.geom, 3857),
                    func.ST_TileEnvelope(tile_path.z, tile_path.x, tile_path.y),
                ),
                PublishedRecord.pharos_id,
                PublishedProject.name.label("project_name"),
            )
            .select_from(PublishedRecord)
            .join(PublishedDataset)
            .join(PublishedProject)
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
            "body": base64.b64encode(tile_bytes).decode('utf-8'),
            'isBase64Encoded': True,
        }
