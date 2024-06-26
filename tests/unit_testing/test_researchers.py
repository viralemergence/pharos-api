from sqlalchemy import URL, create_engine
from sqlalchemy.orm import Session

from models import Base, Researcher
from researchers import get_formatted_researchers


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
                first_name="researcher",
                last_name="1",
                organization="org1",
                email="res1@example.com",
            )
        )
        session.add(
            Researcher(
                researcher_id="res2",
                name="researcher2",
                first_name="researcher",
                last_name="2",
                organization="org2",
                email="res2@example.com",
            )
        )
        session.add(
            Researcher(
                researcher_id="res3",
                name="researcher3",
                first_name="researcher",
                last_name="3",
                organization="org3",
                email="res3@example.com",
            )
        )
        session.commit()

    # get researchers with an empty researcher_ids list
    # should return all researchers
    researchers = get_formatted_researchers(ENGINE, [])
    assert len(researchers["data"]) >= 3

    # get researchers with a list containing one valid researcher_id
    researchers = get_formatted_researchers(ENGINE, ["res1"])
    # should be only one researcher
    assert len(researchers["data"]) == 1
    # check the name to see if it's the right researcher
    assert researchers["data"][0]["name"] == "researcher1"

    # get researchers with a list containing two valid researcher_ids
    researchers = get_formatted_researchers(ENGINE, ["res1", "res2"])
    # should be two researchers
    assert len(researchers["data"]) == 2
    # check the names to see if they're the right researchers
    assert researchers["data"][0]["name"] == "researcher1"
    assert researchers["data"][1]["name"] == "researcher2"

    assert researchers["data"][0]["firstName"] == "researcher"
    assert researchers["data"][0]["lastName"] == "1"
    assert researchers["data"][1]["firstName"] == "researcher"
    assert researchers["data"][1]["lastName"] == "2"
    # get researchers with a list containing one
    # researcher_id which is not in the database
    researchers = get_formatted_researchers(ENGINE, ["res_does_not_exist"])
    assert len(researchers["data"]) == 0
