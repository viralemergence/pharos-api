import boto3

from format import format_response
from published_records_metadata import get_possible_filters
from engine import get_engine

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


def lambda_handler(_, __):
    engine = get_engine()
    possible_filters = get_possible_filters(engine)
    return format_response(
        200,
        {
            "possibleFilters": possible_filters,
        },
    )
