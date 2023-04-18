"""Basic tests for publishing records to the database."""


# from datetime import date
# from typing import Union
# from devtools import debug
from geoalchemy2 import load_spatialite
from sqlalchemy import create_engine, select
from sqlalchemy.event import listen
from sqlalchemy.orm import Session
from publish_register import publish_register_to_session
from register import (
    Dataset,
    Project,
    Register,
    User,
)

from models2 import PublishedRecord, Researcher, Base


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

MOCK_REGISTER = Register.parse_raw(
    # mock json register string from s3
    """
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
                    "dataValue": "105.2705",
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
)

# ENGINE = create_engine("sqlite+pysqlite:///:memory:", echo=True)
# listen(ENGINE, "connect", load_spatialite)
# Base.metadata.create_all(ENGINE)


# def test_researcher():
#     with Session(ENGINE) as session:
#         session.add(
#             Researcher(
#                 researcher_id=JOHN_SMITH.researcher_id,
#                 name=JOHN_SMITH.name,
#             )
#         )
#         session.add(
#             Researcher(
#                 researcher_id=JANE_DOE.researcher_id,
#                 name=JANE_DOE.name,
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


# def test_publish_record():
#     with Session(ENGINE) as session:
#         researchers = session.scalars(
#             select(Researcher).filter(
#                 Researcher.researcher_id.in_(
#                     [JOHN_SMITH.researcher_id, JANE_DOE.researcher_id]
#                 )
#             )
#         ).all()

#         publish_register_to_session(
#             session=session,
#             register=MOCK_REGISTER,
#             project_id=MOCK_PROJECT.project_id,
#             dataset_id=MOCK_DATASET.dataset_id,
#             researchers=list(researchers),
#         )

#         session.commit()

#     with Session(ENGINE) as session:

#         published_record = session.scalars(select(PublishedRecord)).one()
#         assert published_record.pharos_id == (
#             f"{MOCK_PROJECT.project_id}-{MOCK_DATASET.dataset_id}-recAS40712sdgl"
#         )

#         assert len(published_record.researchers) == 2

#         jane = session.scalars(
#             select(Researcher).where(Researcher.researcher_id == JANE_DOE.researcher_id)
#         ).one()

#         assert len(jane.published_records) == 1
#         assert jane.published_records[0].pharos_id == published_record.pharos_id
