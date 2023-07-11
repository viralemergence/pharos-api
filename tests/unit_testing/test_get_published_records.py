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
        host_species = "Mus musculus" if index < 1500 else "Myotis lucifugus"
        collection_year = "2023" if index < 500 else "2024"
        detection_outcome = "positive" if index < 800 else "negative"
        pathogen = "E. coli" if index < 1100 else "Streptococcus pyogenes"
        detection_target = (
            "Salmonella enterica" if index < 400 else "Bordetella pertussis"
        )

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

    assert len(get_query(ENGINE, {"host_species": ["Mus musculus"]}).all()) == 1500
    assert len(get_query(ENGINE, {"host_species": ["Myotis lucifugus"]}).all()) == 500
    assert len(get_query(ENGINE, {"collection_end_date": "2023-1-2"}).all()) == 500
    assert len(get_query(ENGINE, {"collection_start_date": "2023-12-31"}).all()) == 1500

    # Test compound filter
    assert (
        len(
            get_query(
                ENGINE,
                {
                    "collection_start_date": "2023-12-31",
                    "host_species": ["Mus musculus"],
                },
            ).all()
        )
        == 1000
    )

    assert (
        len(
            get_query(
                ENGINE,
                {
                    "collection_end_date": "2023-1-2",
                    "host_species": ["Myotis lucifugus"],
                },
            ).all()
        )
        == 0
    )
