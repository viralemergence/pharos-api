import pytest
import json
from published_records import get_query

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
            detection_target = "Detection target A"
        elif index < 170:
            detection_target = "Detection target B"
        else:
            detection_target = "Detection target C"

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


def check(params, expected_record_count):
    assert len(get_query(ENGINE, params).all()) == expected_record_count


def test_no_filters(mock_data):
    check({}, 400)


def test_filter_by_pharos_id(mock_data):
    check({"pharos_id": "project0-dataset0-rec0"}, 1)
    check({"pharos_id": ["project0-dataset0-rec0", "project1-dataset1-rec1"]}, 2)
    check(
        {"pharos_id": [f"project0-dataset0-rec{num}" for num in range(25)]},
        25,
    )


def test_filter_by_host_species(mock_data):
    check({"host_species": "host1"}, 300)
    check({"host_species": "host2"}, 60)
    check({"host_species": "host3"}, 40)
    check({"host_species": ["host1", "host2"]}, 360)
    check({"host_species": ["host2", "host3"]}, 100)
    check({"host_species": ["host1", "host3"]}, 340)


def test_filter_by_project_name(mock_data):
    check({"project_name": "Project Zero"}, 200)
    # case insensitive
    check({"project_name": "project zero"}, 200)
    # the whole string must match
    check({"project_name": "Zero"}, 0)


def test_filter_by_pathogen(mock_data):
    check({"pathogen": "path1"}, 220)
    check({"pathogen": "path2"}, 60)
    check({"pathogen": "path3"}, 120)
    check({"pathogen": ["path1", "path2"]}, 280)
    check({"pathogen": ["path2", "path3"]}, 180)
    check({"pathogen": ["path1", "path3"]}, 340)


def test_filter_by_project_name_and_host_species(mock_data):
    check({"project_name": "Project Zero", "host_species": "host1"}, 150)
    check({"project_name": "Project Zero", "host_species": "host2"}, 30)
    check({"project_name": "Project Zero", "host_species": "host3"}, 20)
    check({"project_name": "Project Zero", "host_species": ["host1", "host2"]}, 180)
    check({"project_name": "Project Zero", "host_species": ["host2", "host3"]}, 50)
    check({"project_name": "Project Zero", "host_species": ["host1", "host3"]}, 170)


def test_filter_by_host_species_and_pathogen(mock_data):
    check({"host_species": ["host1", "host2"], "pathogen": ["path2", "path3"]}, 140)


def test_filter_by_collection_date(mock_data):
    check({"collection_end_date": "2024-1-2"}, 200)
    check({"collection_start_date": "2024-1-2"}, 200)


def test_filter_by_collection_date_and_host_species(mock_data):
    check({"collection_start_date": "2023-12-31", "host_species": "host1"}, 200)
    check({"collection_end_date": "2023-1-2", "host_species": "host2"}, 0)


def test_filter_by_researcher_name(mock_data):
    check({"researcher": "Researcher Zero"}, 200)
    check({"researcher": "Researcher One"}, 200)
    check({"researcher": "Researcher Two"}, 200)
    check({"researcher": "Researcher Three"}, 200)


def test_filter_by_researcher_name_and_project_name(mock_data):
    check({"researcher": "Researcher Zero", "project_name": "Project Zero"}, 200)
    check({"researcher": "Researcher Zero", "project_name": "Project One"}, 0)


# TODO finish this big test
def skip__test_all_filters(mock_data):
    check(
        {
            "pharos_id": [f"project0-dataset0-rec{num}" for num in range(70)]
            + [f"project1-dataset1-rec{num}" for num in range(80)],
            "project_name": "Project Zero",
            "host_species": ["host1", "host2"],
            "detection_outcome": ["host1", "host2"],
            "collection_start_date": "2023-6-1",
            "collection_end_date": "2024-6-1",
        },
        20,
    )
