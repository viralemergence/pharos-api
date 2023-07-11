import random
import json
from published_records import get_query

from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session
from tests.unit_testing.test_database import (
    MOCK_DATASET,
    MOCK_PROJECT,
    JOHN_SMITH,
    JANE_DOE,
)

from models import PublishedProject, Researcher, Base
from publish_register import (
    create_published_dataset,
    create_published_project,
    create_published_records,
    upsert_project_users,
)

# TODO is this the right approach?
ENGINE = create_engine(
    URL.create(
        drivername="postgresql+psycopg2",
        host="localhost",
        database="pharos-pytest",
        username="postgres",
        port=5432,
        password="1234",
    )
)


Base.metadata.create_all(ENGINE)


def create_mock_register(record_count: int) -> str:
    register_dict = {}
    register_dict["register"] = {}

    for index in range(0, record_count):
        record_id = "rec" + str(index)
        lon = -105.2705 + random.randint(1, 100) / 100
        lat = 40.0150 + random.randint(1, 100) / 100

        # The first 1500 records will have host species "Bat", the rest "Mouse"
        if index < 1500:
            host_species = "Bat"
        else:
            host_species = "Mouse"

        # The first 500 records will have collection date 1/1/2023, the rest
        # 1/1/2024
        if index < 500:
            collection_year = "2023"
        else:
            collection_year = "2024"

        register_dict["register"][record_id] = {
            "Host species": {
                "dataValue": host_species,
                "modifiedBy": "dev",
                "version": "1679692123",
            },
            "Latitude": {
                "dataValue": str(lat),
                "modifiedBy": "dev",
                "version": "1679692123",
            },
            "Longitude": {
                "dataValue": str(lon),
                "modifiedBy": "dev",
                "version": "1679692223",
            },
            "Collection day": {
                "dataValue": "1",
                "modifiedBy": "john",
                "version": "1679692123",
            },
            "Collection month": {
                "dataValue": "1",
                "modifiedBy": "dev",
                "version": "1679692123",
            },
            "Collection year": {
                "dataValue": collection_year,
                "modifiedBy": "dev",
                "version": "1679692123",
            },
            "Detection outcome": {
                "dataValue": "positive",
                "modifiedBy": "dev",
                "version": "1679692123",
            },
        }

    json_register = json.dumps(register_dict)

    return json_register


def test_get_query():
    # Insert mock data
    with Session(ENGINE) as session:
        # Delete all existing projects, if any,
        # so test can be run repeatedly
        session.query(PublishedProject).delete()
        session.commit()

        published_project = create_published_project(
            project=MOCK_PROJECT,
        )

        upsert_project_users(
            session=session,
            published_project=published_project,
            users=[JOHN_SMITH, JANE_DOE],
        )

        for dataset in [MOCK_DATASET]:
            published_dataset = create_published_dataset(
                dataset=dataset,
            )

            mock_register = create_mock_register(2000)

            published_dataset.records = create_published_records(
                register_json=mock_register,
                project_id=published_project.project_id,
                dataset_id=published_dataset.dataset_id,
            )

            published_project.datasets.append(published_dataset)

        session.add(published_project)
        session.commit()

    assert len(get_query(ENGINE, {"host_species": ["Bat"]}).all()) == 1500
    assert len(get_query(ENGINE, {"host_species": ["Mouse"]}).all()) == 500
    assert len(get_query(ENGINE, {"collection_end_date": "2023-1-2"}).all()) == 500
    assert len(get_query(ENGINE, {"collection_start_date": "2023-12-31"}).all()) == 1500
    # TODO: test invalid dates?
