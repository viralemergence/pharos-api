import re
from datetime import datetime
from typing import Optional
import boto3
from pydantic import BaseModel, Extra, Field, ValidationError


from sqlalchemy import and_, or_
from sqlalchemy.orm import Session
from column_alias import API_NAME_TO_UI_NAME_MAP
from engine import get_engine

from format import format_response
from models import PublishedRecord, PublishedDataset, PublishedProject, Researcher
from register import COMPLEX_FIELDS

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


def lambda_handler(event, _):
    try:
        engine = get_engine()

        with Session(engine) as session:
            host_species_list = [
                record.host_species
                for record in session.query(PublishedRecord.host_species)
                .distinct()
                .all()
            ]

        return format_response(
            200,
            {
                "hostSpecies": host_species_list,
            },
        )

    except Exception as e:
        return format_response(500, {"error": str(e)})
