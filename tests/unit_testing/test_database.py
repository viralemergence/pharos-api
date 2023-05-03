"""Basic tests for publishing records to the database."""

import json
import random

from sqlalchemy import URL, create_engine, select
from sqlalchemy.orm import Session
from publish_register import (
    create_published_dataset,
    create_published_project,
    create_published_records,
    upsert_project_users,
)
from register import (
    Dataset,
    Project,
    User,
)

from models import PublishedProject, Researcher, Base


JOHN_SMITH = User.parse_table_item(
    # mock user item from dynamodb
    {
        "pk": "resl90OaJvWZR",
        "sk": "_meta",
        "name": "John Smith",
        "email": "john.smith@institute.org",
        "organization": "Institute of Research",
        "projectIDs": {"prjl90OaJvWZR"},
    }
)

JANE_DOE = User.parse_table_item(
    # mock user item from dynamodb
    {
        "pk": "resl90l123kxd",
        "sk": "_meta",
        "name": "Jane Doe",
        "email": "jane.doe@institute.org",
        "organization": "Institute of Research",
        "projectIDs": {"prjl90OaJvWZR"},
    }
)

MOCK_PROJECT = Project.parse_table_item(
    # mock project item from dynamodb
    {
        "pk": "prjl90OaJvWZR",
        "sk": "_meta",
        "name": "Mock Project",
        "description": "Test project description",
        "authors": [
            {
                "researcherID": "resl90OaJvWZR",
                "role": "Admin",
            },
            {
                "researcherID": "resl90l123kxd",
                "role": "Admin",
            },
        ],
        "citation": "Test citation",
        "datasetIDs": ["setxlj1qoFxLC"],
        "lastUpdated": "2021-01-01",
        "othersCiting": [""],
        "projectPublications": [""],
        "projectType": "",
        "publishStatus": "Unpublished",
        "relatedMaterials": [""],
        "surveillanceStatus": "Ongoing",
    }
)

MOCK_DATASET = Dataset.parse_table_item(
    # mock dataset item from dynamodb
    {
        "pk": "prjl90OaJvWZR",
        "sk": "setxlj1qoFxLC",
        "releaseStatus": "Released",
        "name": "Mock Dataset",
        "lastUpdated": "2021-01-01",
        "earliestDate": "2019-01-01",
        "latestDate": "2020-01-01",
    }
)


def create_mock_register(record_count: int) -> str:

    register_dict = {}
    register_dict["register"] = {}

    for index in range(0, record_count):
        record_id = "rec" + str(index)
        lon = -105.2705 + random.randint(1, 100) / 100
        lat = 40.0150 + random.randint(1, 100) / 100

        register_dict["register"][record_id] = {
            "Host species": {
                "dataValue": "Bat",
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
                "dataValue": "2019",
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


ENGINE = create_engine(
    URL.create(
        drivername="postgresql+psycopg2",
        # host=CREDENTIALS["host"],
        host="localhost",
        database="postgres",
        username="postgres",
        port=5432,
        password="",
    )
)


Base.metadata.create_all(ENGINE)


def test_researcher():
    with Session(ENGINE) as session:
        # Delete all existing projects, if any,
        # so test can be run repeatedly
        session.query(Researcher).delete()
        session.commit()

        session.add(
            Researcher(
                researcher_id=JOHN_SMITH.researcher_id,
                name=JOHN_SMITH.name,
                organization=JOHN_SMITH.organization,
                email=JOHN_SMITH.email,
            )
        )

        session.commit()

    with Session(ENGINE) as session:
        result = session.scalars(
            select(Researcher).where(
                Researcher.researcher_id == JOHN_SMITH.researcher_id
            )
        ).one()

        assert result.researcher_id == JOHN_SMITH.researcher_id
        assert result.name == JOHN_SMITH.name


def test_publish_record():
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

    with Session(ENGINE) as session:

        published_project = session.scalars(select(PublishedProject)).one()

        assert len(published_project.researchers) == 2

        # assert published_project.datasets[0].records[0].pharos_id == (
        #     f"{MOCK_PROJECT.project_id}-{MOCK_DATASET.dataset_id}-recAS40712sdgl"
        # )

        jane = session.scalars(
            select(Researcher).where(Researcher.researcher_id == JANE_DOE.researcher_id)
        ).one()

        assert len(jane.projects) == 1
        assert jane.projects[0].project_id == MOCK_PROJECT.project_id
