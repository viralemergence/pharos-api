"""Basic tests for transforming records into database tests"""

from devtools import debug
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from register import Record

from models2 import Attribution, PublishedRecord, Researcher, Base

VALID_RECORD = """
{
    "Host species": {
        "dataValue": "Vulpes vulpes",
        "modifiedBy": "dev",
        "version": "2",
        "previous": {
            "dataValue": "Previous Data Value",
            "modifiedBy": "dev",
            "version": "1"
        }
    },
    "Age": {
        "dataValue": "10",
        "modifiedBy": "dev",
        "version": "2",
        "previous": {
            "dataValue": "0",
            "modifiedBy": "dev",
            "version": "1"
        }
    },
    "Length": {
        "dataValue": "23",
        "modifiedBy": "dev",
        "version": "2"
    }
}
"""

ENGINE = create_engine("sqlite+pysqlite:///:memory:", echo=True)
Base.metadata.create_all(ENGINE)

JANE_ID = "A098SKHLSD234"


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


TEST_ID = "prjl90OaJvWZR-setxlj1qoFxLC-datJSdfsklklo"


def test_transform_record():
    record = Record.parse_raw(VALID_RECORD)
    debug(record)

    researcher = Researcher(
        researcher_id=JANE_ID,
        first_name="Jane",
        last_name="Doe",
    )

    attribution = Attribution(version="023", researcher=researcher)

    published = PublishedRecord(
        test_id=TEST_ID, attributions=[attribution], **record.__dict__
    )

    with Session(ENGINE) as session:
        session.add(published)
        session.commit()

    with Session(ENGINE) as session:
        published = session.scalars(select(PublishedRecord)).one()
        debug(published)
        debug(published.attributions)
        # debug(test)
