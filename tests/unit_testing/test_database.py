"""Basic tests for transforming records into database tests"""


# from datetime import date
# from typing import Union
# from devtools import debug
# from geoalchemy2 import load_spatialite
# from sqlalchemy import create_engine, select
# from sqlalchemy.event import listen
# from sqlalchemy.orm import Session
# from register import Datapoint, Record, ReportScore

# from models2 import Attribution, PublishedRecord, Researcher, Base

# VALID_RECORD = """
# {
#     "Host species": {
#         "dataValue": "Vulpes vulpes",
#         "modifiedBy": "dev",
#         "version": "2",
#         "previous": {
#             "dataValue": "Previous Data Value",
#             "modifiedBy": "jane",
#             "version": "1",
#             "previous": {
#                 "dataValue": "Old value",
#                 "modifiedBy": "Nathan",
#                 "version": "0"
#             }
#         }
#     },
#     "Latitude": {
#         "dataValue": "40.0150",
#         "modifiedBy": "dev",
#         "version": "1679692123"
#     },
#     "Longitude": {
#         "dataValue": "105.2705",
#         "modifiedBy": "dev",
#         "version": "1679692223"
#     },
#     "Collection day": {
#         "dataValue": "1",
#         "modifiedBy": "john",
#         "version": "1679692123"
#     },
#     "Collection month": {
#         "dataValue": "1",
#         "modifiedBy": "dev",
#         "version": "1679692123"
#     },
#     "Collection year": {
#         "dataValue": "2019",
#         "modifiedBy": "dev",
#         "version": "1679692123"
#     },
#     "Detection outcome": {
#         "dataValue": "positive",
#         "modifiedBy": "dev",
#         "version": "1679692123"
#     },
#     "Pathogen": {
#         "dataValue": "SARS-CoV-2",
#         "modifiedBy": "dev",
#         "version": "1679692123"
#     }
# }
# """

# ENGINE = create_engine("sqlite+pysqlite:///:memory:", echo=True)
# listen(ENGINE, "connect", load_spatialite)
# Base.metadata.create_all(ENGINE)

# JANE_ID = "A098SKHLSD234"


# def test_researcher():
#     with Session(ENGINE) as session:
#         researcher = Researcher(
#             researcher_id=JANE_ID,
#             first_name="Jane",
#             last_name="Doe",
#         )
#         session.add(researcher)
#         session.commit()

#     with Session(ENGINE) as session:
#         result = session.scalars(select(Researcher)).one()

#         print(result)

#         assert result.researcher_id == JANE_ID
#         assert result.first_name == "Jane"
#         assert result.last_name == "Doe"


# PHAROS_ID = "prjl90OaJvWZR-setxlj1qoFxLC-recJSdfsklklo"

# JOHN_ID = "ASDF9098234SD"


# def researchers_from_datapoint(datapoint: Datapoint, researchers: set[str]) -> set[str]:
#     if datapoint.previous is not None:
#         researchers = researchers_from_datapoint(datapoint.previous, researchers)

#     researchers.add(datapoint.modifiedBy)
#     return researchers


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
