import boto3
from sqlalchemy.orm import Session
from models import PublishedProject, PublishedRecord, Researcher
from column_alias import API_NAME_TO_UI_NAME_MAP

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


def get_fields(engine):
    with Session(engine) as session:
        fields = {
            "project_name": {
                "model": PublishedProject,
                "column": "name",
            },
            "researcher_name": {
                "model": Researcher,
                "column": "name",
            },
            "host_species": {
                "model": PublishedRecord,
                "column": "host_species",
            },
            "detection_target": {
                "model": PublishedRecord,
                "column": "detection_target",
            },
            "detection_outcome": {
                "model": PublishedRecord,
                "column": "detection_outcome",
            },
            "pathogen": {
                "model": PublishedRecord,
                "column": "pathogen",
            },
            "collection_start_date": {
                "dataGridKey": "Collection date",
                "type": "date",
                "filterGroup": "collection_date",
            },
            "collection_end_date": {
                "dataGridKey": "Collection date",
                "type": "date",
                "filterGroup": "collection_date",
            },
        }

        for field_name, field in fields.items():
            model = field.get("model")
            column = field.get("column")
            if model and column:
                options = [
                    getattr(record, column)
                    for record in session.query(getattr(model, column))
                    .distinct()
                    .order_by(getattr(model, column))
                    .all()
                ]
                options = [option for option in options if option is not None]
                field["options"] = options
                del field["model"]
                del field["column"]
            # Labels and data grid keys not specified above are determined by column_alias.py
            if "label" not in field and field_name in API_NAME_TO_UI_NAME_MAP:
                field["label"] = API_NAME_TO_UI_NAME_MAP[field_name]
            if "dataGridKey" not in field and field_name in API_NAME_TO_UI_NAME_MAP:
                field["dataGridKey"] = API_NAME_TO_UI_NAME_MAP[field_name]
        return fields
