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

        return format_response(
            200,
            {
                "hostSpecies": host_species,
                "pathogens": pathogens,
                "detectionTargets": detection_targets,
                "detectionOutcomes": detection_outcomes,
            },
        )

    except Exception as e:
        return format_response(500, {"Error": str(e)})
