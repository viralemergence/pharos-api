import boto3
from sqlalchemy import func
from sqlalchemy.orm import Session
from models import PublishedProject, PublishedRecord, Researcher
from column_alias import API_NAME_TO_UI_NAME_MAP

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


def get_possible_filters(engine):
    with Session(engine) as session:
        earliest_date_in_database_string = None
        # 'Latest' as in 'furthest into the future', not as in 'most recent'
        latest_date_in_database_string = None

        # pylint mistakenly rejects func.min and func.max
        # See: https://github.com/sqlalchemy/sqlalchemy/issues/9189
        # pylint: disable=not-callable
        earliest_and_latest_date = session.query(
            func.min(PublishedRecord.collection_date),
            func.max(PublishedRecord.collection_date),
        ).first()

        if earliest_and_latest_date:
            earliest_date_in_database = earliest_and_latest_date[0]
            latest_date_in_database = earliest_and_latest_date[1]
            if earliest_date_in_database and latest_date_in_database:
                earliest_date_in_database_string = earliest_date_in_database.strftime(
                    "%Y-%m-%d"
                )
                latest_date_in_database_string = latest_date_in_database.strftime(
                    "%Y-%m-%d"
                )

        possible_filters = {
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
                "label": "Collection date",
                "dataGridKey": "Collection date",
                "type": "date",
                "earliestDateInDatabase": earliest_date_in_database_string,
                "latestDateInDatabase": latest_date_in_database_string,
            },
            "collection_end_date": {
                "label": "Collection date",
                "dataGridKey": "Collection date",
                "type": "date",
                "earliestDateInDatabase": earliest_date_in_database_string,
                "latestDateInDatabase": latest_date_in_database_string,
            },
        }

        for filter_id, possible_filter in possible_filters.items():
            model = possible_filter.get("model")
            column = possible_filter.get("column")
            if model and column:
                options = [
                    getattr(record, column)
                    for record in session.query(getattr(model, column))
                    .distinct()
                    .order_by(getattr(model, column))
                    .all()
                ]
                options = [option for option in options if option is not None]
                possible_filter["options"] = options
                del possible_filter["model"]
                del possible_filter["column"]
            # Labels and data grid keys not specified above are determined by column_alias.py
            if "label" not in possible_filter and filter_id in API_NAME_TO_UI_NAME_MAP:
                possible_filter["label"] = API_NAME_TO_UI_NAME_MAP[filter_id]
            if (
                "dataGridKey" not in possible_filter
                and filter_id in API_NAME_TO_UI_NAME_MAP
            ):
                possible_filter["dataGridKey"] = API_NAME_TO_UI_NAME_MAP[filter_id]
        return possible_filters


sortable_fields = {
    "collection_date": PublishedRecord.collection_date,
    "project": PublishedProject.name,
    # Not yet supported
    # "researcher_name": PublishedAuthor.name,
    "detection_outcome": PublishedRecord.detection_outcome,
    "sample_id": PublishedRecord.sample_id,
    "animal_id": PublishedRecord.animal_id,
    "host_species": PublishedRecord.host_species,
    "host_species_ncbi_tax_id": PublishedRecord.host_species_ncbi_tax_id,
    "collection_method_or_tissue": PublishedRecord.collection_method_or_tissue,
    "detection_method": PublishedRecord.detection_method,
    "primer_sequence": PublishedRecord.primer_sequence,
    "primer_citation": PublishedRecord.primer_citation,
    "detection_target": PublishedRecord.detection_target,
    "detection_target_ncbi_tax_id": PublishedRecord.detection_target_ncbi_tax_id,
    "detection_outcome": PublishedRecord.detection_outcome,
    "detection_measurement": PublishedRecord.detection_measurement,
    "detection_measurement_units": PublishedRecord.detection_measurement_units,
    "pathogen": PublishedRecord.pathogen,
    "pathogen_ncbi_tax_id": PublishedRecord.pathogen_ncbi_tax_id,
    "genbank_accession": PublishedRecord.genbank_accession,
    "detection_comments": PublishedRecord.detection_comments,
    "organism_sex": PublishedRecord.organism_sex,
    "dead_or_alive": PublishedRecord.dead_or_alive,
    "health_notes": PublishedRecord.health_notes,
    "life_stage": PublishedRecord.life_stage,
    "age": PublishedRecord.age,
    "mass": PublishedRecord.mass,
    "length": PublishedRecord.length,
    "spatial_uncertainty": PublishedRecord.spatial_uncertainty,
}
