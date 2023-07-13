import boto3
from pydantic import BaseModel, Extra, Field, ValidationError

from sqlalchemy.orm import Session
from column_alias import API_NAME_TO_UI_NAME_MAP
from engine import get_engine

from format import format_response
from models import PublishedRecord
from register import COMPLEX_FIELDS


SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")



def lambda_handler(event, _):
	try:
        validated = GetPublishedRecordsEvent.parse_obj(event)
    except ValidationError as e:
        return format_response(400, e.json(), preformatted=True)