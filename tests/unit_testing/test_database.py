"""Basic tests for publishing records to the database."""


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

from models import PublishedProject, PublishedRecord, Researcher, Base


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

# mock json register string from s3
MOCK_REGISTER = """
    {
        "register": {
            "recAS40712sdgl": {
                "Host species": {
                    "dataValue": "Vulpes vulpes",
                    "modifiedBy": "dev",
                    "version": "2",
                    "previous": {
                        "dataValue": "Previous Data Value",
                        "modifiedBy": "jane",
                        "version": "1",
                        "previous": {
                            "dataValue": "Old value",
                            "modifiedBy": "Nathan",
                            "version": "0"
                        }
                    }
                },
                "Latitude": {
                    "dataValue": "40.0150",
                    "modifiedBy": "dev",
                    "version": "1679692123"
                },
                "Longitude": {
                    "dataValue": "-105.2705",
                    "modifiedBy": "dev",
                    "version": "1679692223"
                },
                "Collection day": {
                    "dataValue": "1",
                    "modifiedBy": "john",
                    "version": "1679692123"
                },
                "Collection month": {
                    "dataValue": "1",
                    "modifiedBy": "dev",
                    "version": "1679692123"
                },
                "Collection year": {
                    "dataValue": "2019",
                    "modifiedBy": "dev",
                    "version": "1679692123"
                },
                "Detection outcome": {
                    "dataValue": "positive",
                    "modifiedBy": "dev",
                    "version": "1679692123"
                },
                "Pathogen": {
                    "dataValue": "SARS-CoV-2",
                    "modifiedBy": "dev",
                    "version": "1679692123"
                }
            }
        }
    }
    """


ENGINE = create_engine(
    URL.create(
        drivername="postgresql+psycopg2",
        # host=CREDENTIALS["host"],
        host="localhost",
        database="postgres",
        username="postgres",
        port=5432,
        password="1234",
    )
)


Base.metadata.create_all(ENGINE)


# def test_researcher():
#     with Session(ENGINE) as session:
#         session.add(
#             Researcher(
#                 researcher_id=JOHN_SMITH.researcher_id,
#                 name=JOHN_SMITH.name,
#                 organization=JOHN_SMITH.organization,
#                 email=JOHN_SMITH.email,
#             )
#         )
#         session.add(
#             Researcher(
#                 researcher_id=JANE_DOE.researcher_id,
#                 name=JANE_DOE.name,
#                 organization=JANE_DOE.organization,
#                 email=JANE_DOE.email,
#             )
#         )
#         session.commit()

#     with Session(ENGINE) as session:
#         result = session.scalars(
#             select(Researcher).where(Researcher.researcher_id == JANE_DOE.researcher_id)
#         ).one()

#         print(result)

#         assert result.researcher_id == JANE_DOE.researcher_id
#         assert result.name == "Jane Doe"


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

            published_dataset.records = create_published_records(
                register_json=MOCK_REGISTER,
                project_id=published_project.project_id,
                dataset_id=published_dataset.dataset_id,
            )

            published_project.datasets.append(published_dataset)

        session.add(published_project)
        session.commit()

    with Session(ENGINE) as session:
        published_record = session.scalars(select(PublishedRecord)).one()

        assert published_record.pharos_id == (
            f"{MOCK_PROJECT.project_id}-{MOCK_DATASET.dataset_id}-recAS40712sdgl"
        )

        jane = session.scalars(
            select(Researcher).where(Researcher.researcher_id == JANE_DOE.researcher_id)
        ).one()

        assert len(jane.projects) == 1
        assert jane.projects[0].project_id == MOCK_PROJECT.project_id
