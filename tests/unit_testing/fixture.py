import pytest
import json

from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session

from models import PublishedProject, PublishedRecord, PublishedDataset, Researcher, Base
from register import (
    Dataset,
    Project,
    User,
)
from publish_register import (
    create_published_dataset,
    create_published_project,
    create_published_records,
    upsert_project_users,
)

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


def create_mock_register(record_count: int) -> str:
    register_dict = {}
    register_dict["register"] = {}

    for index in range(0, record_count):
        record_id = "rec" + str(index)
        if index < 150:
            host_species = "host1"
        elif index < 180:
            host_species = "host2"
        else:
            host_species = "host3"

        if index < 50:
            collection_year = "2023"
        elif index < 100:
            collection_year = "2024"
        elif index < 150:
            collection_year = "2025"
        else:
            collection_year = "2026"

        if index < 80:
            detection_outcome = "positive"
        elif index < 190:
            detection_outcome = "negative"
        else:
            detection_outcome = "inconclusive"

        if index < 110:
            pathogen = "path1"
        elif index < 140:
            pathogen = "path2"
        else:
            pathogen = "path3"

        if index < 40:
            detection_target = "target1"
        elif index < 170:
            detection_target = "target2"
        else:
            detection_target = "target3"

        data = {
            "Host species": host_species,
            "Latitude": "-105.2705",
            "Longitude": "40.0150",
            "Collection day": "1",
            "Collection month": "1",
            "Collection year": collection_year,
            "Detection outcome": detection_outcome,
            "Pathogen": pathogen,
            "Detection target": detection_target,
        }
        register_dict["register"][record_id] = {
            field_name: {
                "dataValue": value,
                "modifiedBy": "dev",
                "version": "0000000000",
            }
            for (field_name, value) in data.items()
        }

    json_register = json.dumps(register_dict)

    return json_register


@pytest.fixture
def mock_data():
    Base.metadata.create_all(ENGINE)
    with Session(ENGINE) as session:
        cleanup(session)
        project0 = create_published_project(
            project=Project.parse_table_item(
                {
                    "pk": "project0",
                    "sk": "_meta",
                    "name": "Project Zero",
                    "description": "",
                    "authors": [
                        {
                            "researcherID": "researcher0",
                            "role": "Admin",
                        },
                    ],
                    "citation": "",
                    "datasetIDs": ["dataset0"],
                    "lastUpdated": "2023-01-01",
                    "othersCiting": [""],
                    "projectPublications": [""],
                    "projectType": "",
                    "publishStatus": "Published",
                    "relatedMaterials": [""],
                    "surveillanceStatus": "Ongoing",
                }
            )
        )

        project1 = create_published_project(
            project=Project.parse_table_item(
                {
                    "pk": "project1",
                    "sk": "_meta",
                    "name": "Project One",
                    "description": "",
                    "authors": [
                        {
                            "researcherID": "researcher1",
                            "role": "Admin",
                        },
                    ],
                    "citation": "",
                    "datasetIDs": ["dataset1"],
                    "lastUpdated": "2023-01-01",
                    "othersCiting": [""],
                    "projectPublications": [""],
                    "projectType": "",
                    "publishStatus": "Published",
                    "relatedMaterials": [""],
                    "surveillanceStatus": "Ongoing",
                }
            )
        )

        upsert_project_users(
            session=session,
            published_project=project0,
            users=[
                User.parse_table_item(
                    {
                        "pk": "researcher0",
                        "sk": "_meta",
                        "name": "Researcher Zero",
                        "email": "researcher0@example.com",
                        "organization": "",
                        "projectIDs": {"project0"},
                    }
                ),
                User.parse_table_item(
                    {
                        "pk": "researcher1",
                        "sk": "_meta",
                        "name": "Researcher One",
                        "email": "researcher1@example.com",
                        "organization": "",
                        "projectIDs": {"project0"},
                    }
                ),
            ],
        )

        upsert_project_users(
            session=session,
            published_project=project1,
            users=[
                User.parse_table_item(
                    {
                        "pk": "researcher2",
                        "sk": "_meta",
                        "name": "Researcher Two",
                        "email": "researcher2@example.com",
                        "organization": "",
                        "projectIDs": {"project1"},
                    }
                ),
                User.parse_table_item(
                    {
                        "pk": "researcher3",
                        "sk": "_meta",
                        "name": "Researcher Three",
                        "email": "researcher3@example.com",
                        "organization": "",
                        "projectIDs": {"project1"},
                    }
                ),
            ],
        )

        dataset0 = Dataset.parse_table_item(
            {
                "pk": "dataset0",
                "sk": "dataset0",
                "releaseStatus": "Released",
                "name": "Dataset Zero",
                "lastUpdated": "2021-01-01",
                "earliestDate": "2019-01-01",
                "latestDate": "2020-01-01",
            }
        )
        published_dataset0 = create_published_dataset(
            dataset=dataset0,
        )
        published_dataset0.records = create_published_records(
            register_json=create_mock_register(200),
            project_id=project0.project_id,
            dataset_id=published_dataset0.dataset_id,
        )
        project0.datasets.append(published_dataset0)
        session.add(project0)

        session.commit()

        dataset1 = Dataset.parse_table_item(
            {
                "pk": "dataset1",
                "sk": "dataset1",
                "releaseStatus": "Released",
                "name": "Dataset One",
                "lastUpdated": "2021-01-01",
                "earliestDate": "2019-01-01",
                "latestDate": "2020-01-01",
            }
        )
        published_dataset1 = create_published_dataset(
            dataset=dataset1,
        )
        published_dataset1.records = create_published_records(
            register_json=create_mock_register(200),
            project_id=project1.project_id,
            dataset_id=published_dataset1.dataset_id,
        )
        project1.datasets.append(published_dataset1)
        session.add(project1)

        session.commit()

        yield
        cleanup(session)


def cleanup(session):
    session.query(PublishedProject).delete()
    session.query(PublishedDataset).delete()
    session.query(PublishedRecord).delete()
    session.query(Researcher).delete()
    session.commit()
