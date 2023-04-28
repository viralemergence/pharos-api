from dataclasses import dataclass

from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from engine import get_engine

from devtools import debug

from format import format_response
from models import PublishedRecord


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

    print(tile_path)

    engine = get_engine()

    # func.ST_AsMVT("published_records", "location")

    with Session(engine) as session:
        query = session.query(func.ST_asGeoJSON(PublishedRecord))

        print("query")
        print(query)

        tile = session.execute(query).all()

        points = '{"type": "FeatureCollection", "features": ['
        for point in tile:
            points += point[0] + ","

        points = points[:-1]
        points += "]}"

    return format_response(200, points, preformatted=True)
