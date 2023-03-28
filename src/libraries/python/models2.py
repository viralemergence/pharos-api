from datetime import date
from typing import Optional
from geoalchemy2.types import Geometry, GeometryDump
from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKey,
    Numeric,
    create_engine,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.types import String, TypeDecorator

from devtools import debug


def todict(obj):
    """Return the object's dict excluding private attributes,
    sqlalchemy state and relationship attributes.
    """
    excl = ("_sa_adapter", "_sa_instance_state")
    return {
        k: v
        for k, v in vars(obj).items()
        if not k.startswith("_") and not any(hasattr(v, a) for a in excl)
    }


def wrap_type(value):
    if isinstance(value, str):
        return f'"{value}" {type(value)}'

    return f"{value} {type(value)}"


class Base(DeclarativeBase):
    # Borrowing this automatic __repr__ from
    # https://stackoverflow.com/a/54034230
    def __repr__(self):
        params = ",\n".join(
            f"    {k} = {wrap_type(v)}" for k, v in todict(self).items()
        )
        return f"\n{self.__class__.__name__}(\n{params}\n)"


class CoerceFloat(TypeDecorator):
    """Convert a given value to float"""

    impl = Numeric

    def process_bind_param(self, value, _):
        if value is None:
            return None
        return float(value)


class CoerceStr(TypeDecorator):
    """Convert a given value to string"""

    impl = String

    def process_bind_param(self, value, _):
        if value is None:
            return None
        return str(value)


class CoerceInt(TypeDecorator):
    """Convert a given value to integer"""

    impl = BigInteger

    def process_bind_param(self, value, _):
        if value is None:
            return None
        return int(value)


class Researcher(Base):
    __tablename__ = "researchers"
    researcher_id: Mapped[str] = mapped_column(primary_key=True)
    first_name: Mapped[str]
    last_name: Mapped[str]

    attributions: Mapped[list["Attribution"]] = relationship(
        back_populates="researcher"
    )


class PublishedRecord(Base):
    __tablename__ = "published_records"
    __table_args__ = (
        CheckConstraint("life_stage IN ('adult', 'juvenile', 'neo-natal', 'unknown')"),
        CheckConstraint("organism_sex IN ('female', 'male', 'unknown')"),
        CheckConstraint("dead_or_alive IN ('dead', 'alive', 'unknown')"),
        CheckConstraint(
            "detection_outcome IN ('positive', 'negative', 'inconclusive')"
        ),
    )

    pharos_id: Mapped[str] = mapped_column(primary_key=True)

    sample_id: Mapped[Optional[str]] = mapped_column(CoerceStr)
    animal_id: Mapped[Optional[str]] = mapped_column(CoerceStr)
    host_species: Mapped[str] = mapped_column(CoerceStr)
    host_species_ncbi_tax_id: Mapped[Optional[int]] = mapped_column(CoerceInt)
    # latitude: Mapped[float] = mapped_column(CoerceFloat)
    # longitude: Mapped[float] = mapped_column(CoerceFloat)
    location: Mapped[Geometry] = mapped_column(Geometry("POINT"))
    spatial_uncertainty: Mapped[Optional[str]] = mapped_column(CoerceStr)
    # collection_day: Mapped[int] = mapped_column(CoerceInt)
    # collection_month: Mapped[int] = mapped_column(CoerceInt)
    # collection_year: Mapped[int] = mapped_column(CoerceInt)
    collection_date: Mapped[date]
    collection_method_or_tissue: Mapped[Optional[str]] = mapped_column(CoerceStr)
    detection_method: Mapped[Optional[str]] = mapped_column(CoerceStr)
    primer_sequence: Mapped[Optional[str]] = mapped_column(CoerceStr)
    primer_citation: Mapped[Optional[str]] = mapped_column(CoerceStr)
    detection_target: Mapped[Optional[str]] = mapped_column(CoerceStr)
    detection_target_ncbi_tax_id: Mapped[Optional[int]] = mapped_column(CoerceInt)
    detection_outcome: Mapped[str] = mapped_column(CoerceStr)
    detection_measurement: Mapped[Optional[str]] = mapped_column(CoerceStr)
    detection_measurement_units: Mapped[Optional[str]] = mapped_column(CoerceStr)
    pathogen: Mapped[str] = mapped_column(CoerceStr)
    pathogen_ncbi_tax_id: Mapped[Optional[int]] = mapped_column(CoerceInt)
    genbank_accession: Mapped[Optional[str]] = mapped_column(CoerceStr)
    detection_comments: Mapped[Optional[str]] = mapped_column(CoerceStr)
    organism_sex: Mapped[Optional[str]] = mapped_column(CoerceStr)
    dead_or_alive: Mapped[Optional[str]] = mapped_column(CoerceStr)
    health_notes: Mapped[Optional[str]] = mapped_column(CoerceStr)
    life_stage: Mapped[Optional[str]] = mapped_column(CoerceStr)
    age: Mapped[Optional[int]] = mapped_column(CoerceInt)
    mass: Mapped[Optional[int]] = mapped_column(CoerceInt)
    length: Mapped[Optional[int]] = mapped_column(CoerceInt)

    attributions: Mapped[list["Attribution"]] = relationship(
        back_populates="published_record"
    )


class Attribution(Base):
    __tablename__ = "attributions"
    attribution_id: Mapped[int] = mapped_column(primary_key=True)

    pharos_id: Mapped["PublishedRecord"] = mapped_column(
        ForeignKey("published_records.pharos_id")
    )
    researcher_id: Mapped["Researcher"] = mapped_column(
        ForeignKey("researchers.researcher_id")
    )

    published_record: Mapped["PublishedRecord"] = relationship(
        back_populates="attributions"
    )
    researcher: Mapped["Researcher"] = relationship(back_populates="attributions")

    version: Mapped[int] = mapped_column(CoerceInt)


if __name__ == "__main__":
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=True)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        PHAROS_ID = "prjl90OaJvWZR-setxlj1qoFxLC-datJSdfsklklo"

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

        row = PublishedRecord(
            pharos_id=PHAROS_ID,
            animal_id="fred",
            host_species="bat",
            attributions=[
                Attribution(version="023", researcher=researcher),
                Attribution(version="34", researcher=researcher2),
            ],
        )

        session.add_all([researcher2, researcher, row])
        session.commit()

        statement = select(Researcher)

        for researcher in session.scalars(statement):
            debug(researcher)

        tests = session.scalars(select(PublishedRecord))
        for row in tests:
            debug(row)
            debug(row.attributions)

        print("\nSelecting tests by researcher_id\n")

        tests = session.scalars(
            select(PublishedRecord)
            .join(PublishedRecord.attributions)
            .where(Attribution.researcher_id == "ASDF9098234SD")
        )

        for row in tests:
            debug(row)
            debug(row.animal_id)
