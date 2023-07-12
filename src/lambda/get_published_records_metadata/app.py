import boto3

from format import format_response
from published_records_metadata import get_fields

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


def lambda_handler(_, __):
    fields = get_fields()
    return format_response(
        200,
        {
            "fields": fields,
        },
    )
