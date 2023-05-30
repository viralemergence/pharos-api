import boto3

from sqlalchemy.orm import Session
from engine import get_engine

from format import format_response
from models import PublishedProject, PublishedRecord, Researcher

from column_alias import API_NAME_TO_UI_NAME_MAP

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


def lambda_handler(_, __):
    engine = get_engine()

    with Session(engine) as session:
        fields = {
            "project_name": {
                "label": "Project name",
                "model": PublishedProject,
                "dataGridKey": "Project name",
                "column": "name",
            },
            "researcher_name": {
                "label": "Author",
                "model": Researcher,
                "dataGridKey": "Authors",
                "column": "name",
            },
            "host_species": {
                "label": "Host species",
                "model": PublishedRecord,
                "column": "host_species",
            },
            "detection_target": {
                "label": "Detection target",
                "model": PublishedRecord,
                "column": "detection_target",
            },
            "detection_outcome": {
                "label": "Detection outcome",
                "model": PublishedRecord,
                "column": "detection_outcome",
            },
            "pathogen": {
                "label": "Pathogen",
                "model": PublishedRecord,
                "column": "pathogen",
            },
            "collection_start_date": {
                "label": "Collection start date",
                "dataGridKey": "Collection date",
                "type": "date",
            },
            "collection_end_date": {
                "label": "Collection end date",
                "dataGridKey": "Collection date",
                "type": "date",
            },
        }

        for field in fields:
            model = fields[field].get("model")
            column = fields[field].get("column")
            if model and column:
                options = [
                    getattr(record, column)
                    for record in session.query(getattr(model, column))
                    .distinct()
                    .order_by(getattr(model, column))
                    .all()
                ]
                options = [option for option in options if option is not None]
                fields[field]["options"] = options
                del fields[field]["model"]
                del fields[field]["column"]
            if "dataGridKey" not in fields[field] and field in API_NAME_TO_UI_NAME_MAP:
                fields[field]["dataGridKey"] = API_NAME_TO_UI_NAME_MAP[field]

        return format_response(
            200,
            {
                "fields": fields,
            },
        )
