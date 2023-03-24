from sqlalchemy import BigInteger, ForeignKey, Numeric, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.orm.session import Session

from devtools import debug
from sqlalchemy.types import String, TypeDecorator


class CoerceStr(TypeDecorator):
    """Convert value to string"""

    impl = String

    def process_bind_param(self, value, _):
        if value is None:
            return None
        return str(value)


class CoerceInt(TypeDecorator):
    """Convert value to integer"""

    impl = BigInteger

    def process_bind_param(self, value, _):
        if value is None:
            return None
        return int(value)


class CoerceFloat(TypeDecorator):
    """Convert value to float"""

    impl = Numeric

    def process_bind_param(self, value, _):
        if value is None:
            return None
        return float(value)


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


class PublishedRecord(Base):
    __tablename__ = "test"
    test_id: Mapped[str] = mapped_column(primary_key=True)

    sample_id: Mapped[str] = mapped_column(CoerceStr)
    animal_id: Mapped[str] = mapped_column(CoerceStr)
    host_species: Mapped[str] = mapped_column(CoerceStr)
    host_species_ncbi_tax_id: Mapped[int] = mapped_column(CoerceInt)
    latitude: Mapped[int] = mapped_column(CoerceInt)
    longitude: Mapped[int] = mapped_column(CoerceInt)
    spatial_uncertainty: Mapped[str] = mapped_column(CoerceStr)
    collection_day: Mapped[int] = mapped_column(CoerceInt)
    collection_month: Mapped[int] = mapped_column(CoerceInt)
    collection_year: Mapped[int] = mapped_column(CoerceInt)
    collection_method_or_tissue: Mapped[str] = mapped_column(CoerceStr)
    detection_method: Mapped[str] = mapped_column(CoerceStr)
    primer_sequence: Mapped[str] = mapped_column(CoerceStr)
    primer_citation: Mapped[str] = mapped_column(CoerceStr)
    detection_target: Mapped[str] = mapped_column(CoerceStr)
    detection_target_ncbi_tax_id: Mapped[int] = mapped_column(CoerceInt)
    detection_outcome: Mapped[str] = mapped_column(CoerceStr)
    detection_measurement: Mapped[str] = mapped_column(CoerceStr)
    detection_measurement_units: Mapped[str] = mapped_column(CoerceStr)
    pathogen: Mapped[str] = mapped_column(CoerceStr)
    pathogen_ncbi_tax_id: Mapped[int] = mapped_column(CoerceInt)
    genbank_accession: Mapped[str] = mapped_column(CoerceStr)
    detection_comments: Mapped[str] = mapped_column(CoerceStr)
    organism_sex: Mapped[str] = mapped_column(CoerceStr)
    dead_or_alive: Mapped[str] = mapped_column(CoerceStr)
    health_notes: Mapped[str] = mapped_column(CoerceStr)
    life_stage: Mapped[str] = mapped_column(CoerceStr)
    age: Mapped[int] = mapped_column(CoerceInt)
    mass: Mapped[int] = mapped_column(CoerceInt)
    length: Mapped[int] = mapped_column(CoerceInt)

    attributions: Mapped[list["Attribution"]] = relationship(back_populates="test")

    def __repr__(self):
        return (
            f"  test_id:        {self.test_id}\n"
            f"  length:         {self.length}\n"
            f"  host_species:   {self.host_species}\n"
        )


class Attribution(Base):
    __tablename__ = "attribution"
    attribution_id: Mapped[int] = mapped_column(primary_key=True)

    test_id: Mapped["PublishedRecord"] = mapped_column(ForeignKey("test.test_id"))
    researcher_id: Mapped["Researcher"] = mapped_column(
        ForeignKey("researcher.researcher_id")
    )

    test: Mapped["PublishedRecord"] = relationship(back_populates="attributions")
    researcher: Mapped["Researcher"] = relationship(back_populates="attributions")

    version: Mapped[int] = mapped_column(CoerceInt)

    def __repr__(self):
        return (
            f"  test_id:       {self.test_id}\n"
            f"  researcher_id: {self.researcher_id}\n"
            f"  version:       {self.version}\n"
        )


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

        test = PublishedRecord(
            test_id=TEST_ID,
            animal_id="fred",
            host_species="bat",
            attributions=[
                Attribution(version="023", researcher=researcher),
                Attribution(version="34", researcher=researcher2),
            ],
        )

        session.add_all([researcher2, researcher, test])
        session.commit()

        statement = select(Researcher)

        for researcher in session.scalars(statement):
            debug(researcher)
            # debug(researcher.attributions)

        tests = session.scalars(select(PublishedRecord))
        for test in tests:
            debug(test)
            debug(test.attributions)

        print("\nSelecting tests by researcher_id\n")

        tests = session.scalars(
            select(PublishedRecord)
            .join(PublishedRecord.attributions)
            .where(Attribution.researcher_id == "ASDF9098234SD")
        )

        for test in tests:
            debug(test)
