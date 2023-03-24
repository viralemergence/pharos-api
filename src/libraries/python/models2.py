from sqlalchemy import BigInteger, ForeignKey, Numeric, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.orm.session import Session

from devtools import debug
from sqlalchemy import types


class Base(DeclarativeBase):
    pass


class Researcher(Base):
    __tablename__ = "researcher"
    researcher_id: Mapped[str] = mapped_column(primary_key=True)
    first_name: Mapped[str]
    last_name: Mapped[str]

    attributions: Mapped[list["Attribution"]] = relationship(
        back_populates="researcher"
    )

    def __repr__(self):
        return (
            f"  researcher_id:  {self.researcher_id}\n"
            f"  name:           {self.first_name} {self.last_name}\n"
        )


class Test(Base):
    __tablename__ = "test"
    test_id: Mapped[str] = mapped_column(primary_key=True)
    animal_id: Mapped[str]
    host_species: Mapped[str]

    attributions: Mapped[list["Attribution"]] = relationship(back_populates="test")

    def __repr__(self):
        return (
            f"  test_id:        {self.test_id}\n"
            f"  animal_id:      {self.animal_id}\n"
            f"  host_species:   {self.host_species}\n"
        )


class CoerceInt(types.TypeDecorator):
    """Convert value to integer"""

    impl = BigInteger

    def process_bind_param(self, value, _):
        return int(value)


class CoerceFloat(types.TypeDecorator):
    """Convert value to float"""

    impl = Numeric

    def process_bind_param(self, value, _):
        return float(value)


class Attribution(Base):
    __tablename__ = "attribution"
    attribution_id: Mapped[int] = mapped_column(primary_key=True)

    test_id: Mapped["Test"] = mapped_column(ForeignKey("test.test_id"))
    researcher_id: Mapped["Researcher"] = mapped_column(
        ForeignKey("researcher.researcher_id")
    )

    test: Mapped["Test"] = relationship(back_populates="attributions")
    researcher: Mapped["Researcher"] = relationship(back_populates="attributions")

    version: Mapped[int] = mapped_column(CoerceInt)

    def __repr__(self):
        return (
            f"  test_id:       {self.test_id}\n"
            f"  researcher_id: {self.researcher_id}\n"
            f"  version:       {self.version}\n"
        )


class IdTest:
    def __init__(self, test_id):
        self.test_id = test_id
        self.other = "other"

    def __int__(self):
        return int(self.test_id)


if __name__ == "__main__":
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        TEST_ID = "prjl90OaJvWZR-setxlj1qoFxLC-datJSdfsklklo"

        researcher = Researcher(
            researcher_id="A098SKHLSD234",
            first_name="Jane",
            last_name="Doe",
        )
        researcher2 = Researcher(
            researcher_id="ASDF9098234SD",
            first_name="John",
            last_name="Smith",
        )

        testid = IdTest(test_id="45")

        test = Test(
            test_id=TEST_ID,
            animal_id="fred",
            host_species="bat",
            attributions=[
                Attribution(version="023", researcher=researcher),
                Attribution(version=testid, researcher=researcher2),
            ],
        )

        session.add_all([researcher2, researcher, test])
        session.commit()

        statement = select(Researcher)

        for researcher in session.scalars(statement):
            debug(researcher)
            # debug(researcher.attributions)

        tests = session.scalars(select(Test))
        for test in tests:
            debug(test)
            debug(test.attributions)

        print("\nSelecting tests by researcher_id\n")

        tests = session.scalars(
            select(Test)
            .join(Test.attributions)
            .where(Attribution.researcher_id == "ASDF9098234SD")
        )

        for test in tests:
            debug(test)
