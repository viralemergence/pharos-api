"""Basic tests for publishing records to the database."""


# from datetime import date
# from typing import Union
# from devtools import debug
from geoalchemy2 import load_spatialite
from sqlalchemy import create_engine, select
from sqlalchemy.event import listen
from sqlalchemy.orm import Session
from register import (
    Dataset,
    Project,
    Register,
    User,
)

from models2 import Researcher, Base


# mock user item from dynamodb
JOHN_SMITH = User.parse_table_item(
    {
        "pk": "resl90OaJvWZR",
        "sk": "_meta",
        "name": "John Smith",
        "email": "john.smith@institute.org",
        "organization": "Institute of Research",
        "projectIDs": {"prjl90OaJvWZR"},
    }
)

# mock user item from dynamodb
JANE_DOE = User.parse_table_item(
    {
        "pk": "resl90l123kxd",
        "sk": "_meta",
        "name": "Jane Doe",
        "email": "jane.doe@institute.org",
        "organization": "Institute of Research",
        "projectIDs": {"prjl90OaJvWZR"},
    }
)

# mock project item from dynamodb
MOCK_PROJECT = Project.parse_table_item(
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

# mock dataset item from dynamodb
MOCK_DATASET = Dataset.parse_table_item(
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

# mock register json string from s3
MOCK_REGISTER = Register.parse_raw(
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

ENGINE = create_engine("sqlite+pysqlite:///:memory:", echo=True)
listen(ENGINE, "connect", load_spatialite)
Base.metadata.create_all(ENGINE)


def test_researcher():
    with Session(ENGINE) as session:
        session.add(
            Researcher(
                researcher_id=JOHN_SMITH.researcher_id,
                name=JOHN_SMITH.name,
            )
        )
        session.add(
            Researcher(
                researcher_id=JANE_DOE.researcher_id,
                name=JANE_DOE.name,
            )
        )
        session.commit()

    with Session(ENGINE) as session:
        result = session.scalars(
            select(Researcher).where(Researcher.researcher_id == JANE_DOE.researcher_id)
        ).one()

        print(result)

        assert result.researcher_id == JANE_DOE.researcher_id
        assert result.name == "Jane Doe"


# def test_transform_record():
#     # with Session(ENGINE) as session:
#     #     john = Researcher(
#     #         researcher_id=JOHN_ID,
#     #         first_name="John",
#     #         last_name="Smith",
#     #     )
#     #     session.add(john)
#     #     session.commit()

#     with Session(ENGINE) as session:
#         record = Record.parse_raw(VALID_RECORD)

#         # attribution = Attribution(version="023")

#         # john = session.scalar(
#         #     select(Researcher).where(Researcher.researcher_id == JOHN_ID)
#         # )

#         # if john is None:
#         #     john = Researcher(
#         #         researcher_id=JOHN_ID,
#         #         first_name="John",
#         #         last_name="Smith",
#         #     )

#         # john.attributions.append(attribution)
#         record_dict: dict[str, Datapoint] = record.__dict__

#         exclude = {
#             "collection_day",
#             "collection_month",
#             "collection_year",
#             "latitude",
#             "longitude",
#         }

#         prepublish: dict[str, Union[Datapoint, date]] = {}
#         researchers: set[str] = set()
#         attributions: list[Attribution] = []

#         for field, datapoint in record_dict.items():
#             if datapoint is not None:
#                 researchers = researchers_from_datapoint(datapoint, researchers)

#                 if field not in exclude:
#                     prepublish[field] = datapoint

#         debug(prepublish)
#         debug(researchers)

#         if (
#             not record.collection_day
#             or not record.collection_month
#             or not record.collection_year
#         ):
#             raise ValueError(
#                 "Record is missing collection date, should not have passed validator"
#             )

#         collection_date = date(
#             int(record.collection_year),
#             int(record.collection_month),
#             int(record.collection_day),
#         )

#         if not record.latitude or not record.longitude:
#             raise ValueError(
#                 "Record is missing location, should not have passed validator"
#             )

#         location = f"POINT({record.latitude},{record.longitude})"

#         published = PublishedRecord(
#             pharos_id=PHAROS_ID,
#             attributions=attributions,
#             collection_date=collection_date,
#             location=location,
#             **prepublish,
#         )

#         session.add(published)
#         session.commit()

#     with Session(ENGINE) as session:
#         published = session.scalars(select(PublishedRecord)).one()
#         debug(published)
#         debug(published.attributions)

#         # john = session.scalar(
#         #     select(Researcher).where(Researcher.researcher_id == JOHN_ID)
#         # )

#         # if john:
#         #     debug(john)
#         #     debug(john.attributions)

#         researchers_result = session.scalars(select(Researcher)).all()

#         for researcher in researchers_result:
#             debug(researcher)
#             # if researcher.attributions:
#             #     debug(researcher.attributions)
