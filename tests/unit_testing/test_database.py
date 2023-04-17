"""Basic tests for publishing records to the database."""


# from datetime import date
# from typing import Union
# from devtools import debug
from geoalchemy2 import load_spatialite
from sqlalchemy import create_engine, select
from sqlalchemy.event import listen
from sqlalchemy.orm import Session
from register import (
    Author,
    Datapoint,
    Dataset,
    DatasetReleaseStatus,
    Project,
    ProjectAuthorRole,
    ProjectPublishStatus,
    Register,
    User,
)

from models2 import Attribution, PublishedRecord, Researcher, Base

MOCK_PROJECT_ID = "prjl90OaJvWZR"


JOHN_SMITH = User(
    researcherID="resl90OaJvWZR",
    name="John Smith",
    email="john.smith@institute.org",
    organization="Institute of Research",
    projectIDs={MOCK_PROJECT_ID},
)

JANE_DOE = User(
    researcherID="resl90l123kxd",
    name="Jane Doe",
    email="jane.doe@institute.org",
    organization="Institute of Research",
    projectIDs={MOCK_PROJECT_ID},
)

JOHN_AUTHOR = Author(
    researcherID=JOHN_SMITH.researcherID,
    role=ProjectAuthorRole.ADMIN,
)

JANE_AUTHOR = Author(
    researcherID=JANE_DOE.researcherID,
    role=ProjectAuthorRole.ADMIN,
)


MOCK_PROJECT = Project(
    projectID=MOCK_PROJECT_ID,
    name="Mock Project",
    description="Test project description",
    authors=[JANE_AUTHOR, JOHN_AUTHOR],
    citation="Test citation",
    datasetIDs=["setxlj1qoFxLC"],
    lastUpdated="2021-01-01",
    othersCiting=[""],
    projectPublications=[""],
    projectType="",
    publishStatus=ProjectPublishStatus.UNPUBLISHED,
    relatedMaterials=[""],
    surveillanceStatus="Ongoing",
)

MOCK_DATASET = Dataset(
    datasetID="setxlj1qoFxLC",
    projectID=MOCK_PROJECT_ID,
    releaseStatus=DatasetReleaseStatus.RELEASED,
    name="Mock Dataset",
    lastUpdated="2021-01-01",
    earliestDate="2019-01-01",
    latestDate="2020-01-01",
)

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

JANE_ID = "A098SKHLSD234"


def test_researcher():
    with Session(ENGINE) as session:
        researcher = Researcher(
            researcher_id=JANE_ID,
            first_name="Jane",
            last_name="Doe",
        )
        session.add(researcher)
        session.commit()

    with Session(ENGINE) as session:
        result = session.scalars(select(Researcher)).one()

        print(result)

        assert result.researcher_id == JANE_ID
        assert result.first_name == "Jane"
        assert result.last_name == "Doe"


def researchers_from_datapoint(datapoint: Datapoint, researchers: set[str]) -> set[str]:
    if datapoint.previous is not None:
        researchers = researchers_from_datapoint(datapoint.previous, researchers)

    researchers.add(datapoint.modifiedBy)
    return researchers


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
