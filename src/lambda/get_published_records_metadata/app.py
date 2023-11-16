from column_alias import API_NAME_TO_UI_NAME_MAP
from engine import get_engine
from format import format_response
from published_records_metadata import get_possible_filters, sortable_fields


def lambda_handler(_, __):
    engine = get_engine()
    possible_filters = get_possible_filters(engine)
    ui_names_of_sortable_fields = [
        API_NAME_TO_UI_NAME_MAP.get(field_name, field_name)
        for field_name in sortable_fields
    ]
    return format_response(
        200,
        {
            "possibleFilters": possible_filters,
            "sortableFields": ui_names_of_sortable_fields,
        },
    )
