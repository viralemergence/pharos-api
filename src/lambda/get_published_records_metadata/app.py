import boto3

from sqlalchemy.orm import Session
from engine import get_engine

from format import format_response
from models import PublishedRecord

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


def lambda_handler(event, _):
    try:
        engine = get_engine()

        with Session(engine) as session:
            pathogens = [
                record.pathogen
                for record in session.query(PublishedRecord.pathogen).distinct().all()
            ]
            host_species = [
                record.host_species
                for record in session.query(PublishedRecord.host_species)
                .distinct()
                .all()
            ]
            detection_targets = [
                record.detection_target
                for record in session.query(PublishedRecord.detection_target)
                .distinct()
                .all()
            ]
            detection_outcomes = [
                record.detection_outcome
                for record in session.query(PublishedRecord.detection_outcome)
                .distinct()
                .all()
            ]

        options_for_fields = {
            "hostSpecies": host_species,
            "pathogen": pathogens,
            "detectionTarget": detection_targets,
            "detectionOutcome": detection_outcomes,
        }

        # Remove null values
        options_for_fields = {
            key: [option for option in options if option is not None]
            for key, options in options_for_fields.items()
        }

        return format_response(
            200,
            {"optionsForFields": options_for_fields},
        )

    except Exception as e:
        return format_response(500, {"Error": str(e)})
