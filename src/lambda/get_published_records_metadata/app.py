import boto3

from format import format_response
from published_records_metadata import get_possible_filters, sortable_fields
from column_alias import UI_NAME_TO_API_NAME_MAP
from engine import get_engine

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


def lambda_handler(_, __):
    engine = get_engine()
    possible_filters = get_possible_filters(engine)
    sortable_fields_using_ui_names = {
        (UI_NAME_TO_API_NAME_MAP.get(key) or key, value)
        for key, value in sortable_fields
    }
    return format_response(
        200,
        {
            "possibleFilters": possible_filters,
            "sortableFields": sortable_fields_using_ui_names,
        },
    )
