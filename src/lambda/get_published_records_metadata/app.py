from format import format_response
from published_records_metadata import get_possible_filters
from engine import get_engine


def lambda_handler(_, __):
    engine = get_engine()
    possible_filters = get_possible_filters(engine)
    return format_response(
        200,
        {
            "possibleFilters": possible_filters,
        },
    )
