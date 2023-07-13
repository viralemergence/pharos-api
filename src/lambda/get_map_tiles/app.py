# from dataclasses import dataclass
# import time
from sqlalchemy import select

from sqlalchemy.orm import Session
from sqlalchemy.types import STRINGTYPE

from sqlalchemy.sql import cast, func
from sqlalchemy.types import JSON
from engine import get_engine

from format import format_response
from models import PublishedRecord


## wrote this for doing tiles;
## commented out while this is only
## returning geojson as proof of concept

# @dataclass
# class TilePath:
#     """Validate and parse the path of a map tile request."""

#     z: int
#     x: int
#     y: int

#     @classmethod
#     def validate(cls, path):
#         try:
#             coords, extension = path.split(".")
#             z, x, y = [int(chunk) for chunk in coords.split("/")]

#         except ValueError as exc:
#             raise ValueError(
#                 "Incorrect format; Map tile requests "
#                 "must be in the format /map/z/x/y.pbf"
#             ) from exc

#         if extension != "pbf":
#             raise ValueError(
#                 f"Incorrect extension '.{extension}'; Map tile "
#                 "requests must be in the format /map/z/x/y.pbf"
#             )

#         min_z, max_z = 0, 20
#         if not min_z <= int(z) <= max_z:
#             raise ValueError(
#                 "Unsupported zoom level; zoom level "
#                 f"must be between {min_z} and {max_z}"
#             )

#         return cls(z, x, y)


def lambda_handler(_, __):
    # try:
    #     tile_path = TilePath.validate(event["pathParameters"]["mapPath"])
    # except ValueError as exc:
    #     return format_response(400, {"error": str(exc)})

    engine = get_engine()
    with Session(engine) as session:

        data = session.scalar(
            select(
                cast(
                    func.json_build_object(
                        "type",
                        "FeatureCollection",
                        "features",
                        func.json_agg(
                            cast(
                                func.ST_AsGeoJSON(
                                    select(
                                        PublishedRecord.geom,
                                        PublishedRecord.pharos_id,
                                        PublishedRecord.sample_id,
                                        PublishedRecord.collection_date,
                                    ).subquery()
                                ),
                                JSON,
                            )
                        ),
                    ),
                    STRINGTYPE,
                )
            )
        )

        if data is None:
            return format_response(404, {"error": "Not found"})

        return format_response(200, data, preformatted=True)
