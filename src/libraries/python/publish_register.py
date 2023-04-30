from datetime import datetime, date
import json
import time
from geoalchemy2 import WKTElement
from sqlalchemy.orm import Session
from column_alias import get_api_name
from models import PublishedDataset, PublishedProject, PublishedRecord, Researcher
from register import COMPLEX_FIELDS, Datapoint, Dataset, Project, Record, User


def create_published_project(project: Project) -> PublishedProject:
    """Create a new published project from a project object"""

    published_project = PublishedProject()
    published_project.project_id = project.project_id
    published_project.name = project.name
    published_project.description = project.description
    published_project.published_date = datetime.utcnow().date()

    return published_project


def create_published_dataset(dataset: Dataset) -> PublishedDataset:

    published_dataset = PublishedDataset()
    published_dataset.dataset_id = dataset.dataset_id
    published_dataset.project_id = dataset.project_id
    published_dataset.name = dataset.name

    return published_dataset


def upsert_project_users(
    session: Session, published_project: PublishedProject, users: list[User]
) -> None:

    """Get existing researchers from the database and add new ones."""

    start = time.time()

    researcher_ids = [user.researcher_id for user in users]

    # Get existing researchers from database
    existing_researchers = (
        session.query(Researcher)
        .filter(Researcher.researcher_id.in_(researcher_ids))
        .all()
    )

    print("Query existing researchers", time.time() - start)
    start = time.time()

    existing_researcher_ids = [r.researcher_id for r in existing_researchers]

    new_researchers: list[Researcher] = []

    # Create new researchers
    for user in users:
        if user.researcher_id in existing_researcher_ids:
            continue

        new_researchers.append(
            Researcher(
                researcher_id=user.researcher_id,
                name=user.name,
                organization=user.organization,
                email=user.email,
            )
        )

    published_project.researchers = existing_researchers + new_researchers

    print("Add new researchers", time.time() - start)


def create_published_records(
    register_json: str, project_id: str, dataset_id: str
) -> list[PublishedRecord]:

    """Transform a register json string into a list of PublishedRecord objects."""

    register_dict = json.loads(register_json)
    published_records = []

    for record_id, record_dict in register_dict["register"].items():

        # create a PublishedRecord database model with just ID columns
        published_record = PublishedRecord()
        published_record.pharos_id = project_id + "-" + dataset_id + "-" + record_id
        published_record.dataset_id = dataset_id

        # construct a blank record with no fields or validation
        record = Record.construct()

        for field, datapoint_dict in record_dict.items():
            # translate the column name to the record field name
            # because the construct() method skips aliasing
            api_field = get_api_name(field)

            # construct the top-level, current version of the
            # datapoint object with no validation or history
            datapoint = Datapoint.construct(**datapoint_dict)

            # add complex fields to the Record
            # for additional procesing
            if api_field in COMPLEX_FIELDS:
                setattr(record, api_field, datapoint)

            # add simple fields directly to the
            # PublishedRecord database model.
            else:
                setattr(published_record, api_field, datapoint)

        # extra guard for missing fields; shouldn't
        # be able to get here without them fields
        if (
            not record.collection_day
            or not record.collection_month
            or not record.collection_year
        ):
            raise ValueError(
                "Record is missing collection date, should not have passed validator"
            )

        # create and add the collection_date
        # object to the database model
        published_record.collection_date = date(
            int(record.collection_year),
            int(record.collection_month),
            int(record.collection_day),
        )

        # extra guard for missing fields; shouldn't
        # be able to get here without them fields
        if not record.latitude or not record.longitude:
            raise ValueError(
                "Record is missing location, should not have passed validator"
            )

        # create and add the location WKT
        # geometry string to the database model
        published_record.location = WKTElement(
            f"POINT({record.longitude} {record.latitude})"
        )

        # add the researchers to the published record
        # published_record.researchers.extend(researchers)

        published_records.append(published_record)

    return published_records
