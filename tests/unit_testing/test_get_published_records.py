import json
from published_records import get_query

from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session

from models import PublishedProject, Researcher, Base
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
        if index < 1500:
            host_species = "host1"
        elif index < 1800:
            host_species = "host2"
        else:
            host_species = "host3"

        if index < 500:
            collection_year = "2023"
        else:
            collection_year = "2024"

        if index < 800:
            detection_outcome = "positive"
        elif index < 1900:
            detection_outcome = "negative"
        else:
            detection_outcome = "inconclusive"

        if index < 1100:
            pathogen = "path1"
        elif index < 1400:
            pathogen = "path2"
        else:
            pathogen = "path3"

        if index < 400:
            detection_target = "Detection target A"
        elif index < 1700:
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


def test_get_query():
    # Insert mock data
    with Session(ENGINE) as session:
        # Delete all existing projects, if any,
        # so test can be run repeatedly
        session.query(PublishedProject).delete()
        session.commit()

        project_0 = create_published_project(
            project=Project.parse_table_item(
                {
                    "pk": "0",
                    "sk": "_meta",
                    "name": "Project Zero",
                    "description": "",
                    "authors": [
                        {
                            "researcherID": "researcher_0",
                            "role": "Admin",
                        },
                        {
                            "researcherID": "researcher_1",
                            "role": "Admin",
                        },
                    ],
                    "citation": "",
                    "datasetIDs": ["dataset_0"],
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
            published_project=project_0,
            users=[
                User.parse_table_item(
                    {
                        "pk": "researcher_0",
                        "sk": "_meta",
                        "name": "Researcher Zero",
                        "email": "researcher_0@example.com",
                        "organization": "",
                        "projectIDs": {"project_0"},
                    }
                ),
                User.parse_table_item(
                    {
                        "pk": "researcher_1",
                        "sk": "_meta",
                        "name": "Researcher One",
                        "email": "researcher_1@example.com",
                        "organization": "",
                        "projectIDs": {"project_0"},
                    }
                ),
            ],
        )

        mock_dataset = Dataset.parse_table_item(
            {
                "pk": "dataset_0",
                "sk": "dataset_0",
                "releaseStatus": "Released",
                "name": "Dataset Zero",
                "lastUpdated": "2021-01-01",
                "earliestDate": "2019-01-01",
                "latestDate": "2020-01-01",
            }
        )

        published_dataset = create_published_dataset(
            dataset=mock_dataset,
        )
        mock_register = create_mock_register(2000)
        published_dataset.records = create_published_records(
            register_json=mock_register,
            project_id=project_0.project_id,
            dataset_id=published_dataset.dataset_id,
        )
        project_0.datasets.append(published_dataset)
        session.add(project_0)
        session.commit()

    def test(params, expected_record_count):
        assert len(get_query(ENGINE, params).all()) == expected_record_count

    test({"host_species": "host1"}, 1500)
    test({"host_species": "host2"}, 300)
    test({"host_species": "host3"}, 200)
    test({"host_species": ["host1", "host2"]}, 1800)
    test({"host_species": ["host2", "host3"]}, 500)
    test({"host_species": ["host1", "host3"]}, 1700)

    test({"pathogen": "path1"}, 1100)
    test({"pathogen": "path2"}, 300)
    test({"pathogen": "path3"}, 600)
    test({"pathogen": ["path1", "path2"]}, 1400)
    test({"pathogen": ["path2", "path3"]}, 900)
    test({"pathogen": ["path1", "path3"]}, 1700)

    test(
        {
            "host_species": ["host1", "host2"],
            "pathogen": ["path2", "path3"],
        },
        700,
    )

    test({"collection_end_date": "2023-1-2"}, 500)
    test({"collection_start_date": "2023-12-31"}, 1500)
    test({"collection_start_date": "2023-12-31", "host_species": "host1"}, 1000)
    test({"collection_end_date": "2023-1-2", "host_species": "host2"}, 0)
