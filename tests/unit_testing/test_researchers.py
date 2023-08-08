from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session

from models import Base, Researcher
from researchers import get_researchers


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


def test_get_researcher():
    # Add some researchers to the database for the tests
    with Session(ENGINE) as session:
        session.add(
            Researcher(
                researcher_id="res1",
                name="researcher1",
                organization="org1",
                email="res1@example.com",
            )
        )
        session.add(
            Researcher(
                researcher_id="res2",
                name="researcher2",
                organization="org2",
                email="res2@example.com",
            )
        )
        session.add(
            Researcher(
                researcher_id="res3",
                name="researcher3",
                organization="org3",
                email="res3@example.com",
            )
        )
        session.commit()

    # get researchers with an empty researcher_ids list
    # should return all researchers
    researchers = get_researchers(ENGINE, [])
    assert len(researchers) >= 3

    # get researchers with a list containing one valid researcher_id
    researchers = get_researchers(ENGINE, ["res1"])
    # should be only one researcher
    assert len(researchers) == 1
    # check the name to see if it's the right researcher
    assert researchers[0]["name"] == "researcher1"

    # get researchers with a list containing two valid researcher_ids
    researchers = get_researchers(ENGINE, ["res1", "res2"])
    # should be two researchers
    assert len(researchers) == 2
    # check the names to see if they're the right researchers
    assert researchers[0]["name"] == "researcher1"
    assert researchers[1]["name"] == "researcher2"

    # get researchers with a list containing one
    # researcher_id which is not in the database
    researchers = get_researchers(ENGINE, ["res_does_not_exist"])
    assert len(researchers) == 0
