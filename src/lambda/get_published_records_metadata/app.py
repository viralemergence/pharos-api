import boto3

from sqlalchemy.orm import Session
from engine import get_engine

from format import format_response
from models import PublishedProject, PublishedRecord, Researcher

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


def lambda_handler(_, __):
    try:
        engine = get_engine()

        with Session(engine) as session:
            researcher_names = [
                record.name
                for record in session.query(Researcher.name)
                .distinct()
                .order_by(Researcher.name)
                .all()
            ]
            pathogens = [
                record.pathogen
                for record in session.query(PublishedRecord.pathogen)
                .distinct()
                .order_by(PublishedRecord.pathogen)
                .all()
            ]
            host_species = [
                record.host_species
                for record in session.query(PublishedRecord.host_species)
                .distinct()
                .order_by(PublishedRecord.host_species)
                .all()
            ]
            detection_targets = [
                record.detection_target
                for record in session.query(PublishedRecord.detection_target)
                .distinct()
                .order_by(PublishedRecord.detection_target)
                .all()
            ]
            detection_outcomes = [
                record.detection_outcome
                for record in session.query(PublishedRecord.detection_outcome)
                .order_by(PublishedRecord.detection_outcome)
                .distinct()
                .all()
            ]
            project_names = [
                record.name
                for record in session.query(PublishedProject.name)
                .distinct()
                .order_by(PublishedProject.name)
                .all()
            ]

        options_for_fields = {
            "hostSpecies": host_species,
            "pathogen": pathogens,
            "detectionTarget": detection_targets,
            "detectionOutcome": detection_outcomes,
            "researcherName": researcher_names,
            "projectName": project_names,
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
        return format_response(500, {"error": str(e)})
