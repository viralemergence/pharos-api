from datetime import date
from typing import Optional
from geoalchemy2 import WKTElement
from geoalchemy2.types import Geometry
from sqlalchemy import (
    BigInteger,
    Column,
    ForeignKey,
    Numeric,
    Table,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import String, TypeDecorator


from value_alias import (
    DeadOrAlive,
    DetectionOutcome,
    OrganismSex,
    DEAD_OR_ALIVE_VALUES_MAP,
    DETECTION_OUTCOME_VALUES_MAP,
    ORGANISM_SEX_VALUES_MAP,
)


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
    """Convert a given value to float, or return None"""

    impl = Numeric
    cache_ok = True

    def process_bind_param(self, value, _):
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None


class CoerceStr(TypeDecorator):
    """Convert a given value to string, or return None"""

    impl = String
    cache_ok = True

    def process_bind_param(self, value, _):
        if value is None:
            return None
        try:
            return str(value)
        except ValueError:
            return None


class CoerceInt(TypeDecorator):
    """Convert a given value to integer, or return None"""

    impl = BigInteger
    cache_ok = True

    def process_bind_param(self, value, _):
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None


class AliasOrganismSex(TypeDecorator):
    """Convert allowed organism_sex datapoint to standardized database value"""

    impl = String
    cache_ok = True

    def process_bind_param(self, value, _):
        if value is None:
            return None
        return ORGANISM_SEX_VALUES_MAP[str(value).lower()]


class AliasDetectionOutcome(TypeDecorator):
    """Convert allowed detection_outcome datapoint to standardized database value"""

    impl = String
    cache_ok = True

    def process_bind_param(self, value, _):
        if value is None:
            return None
        return DETECTION_OUTCOME_VALUES_MAP[str(value).lower()]


class AliasDeadOrAlive(TypeDecorator):
    """Convert allowed dead_or_alive datapoint to standardized database value"""

    impl = String
    cache_ok = True

    def process_bind_param(self, value, _):
        if value is None:
            return None
        return DEAD_OR_ALIVE_VALUES_MAP[str(value).lower()]


projects_researchers = Table(
    "projects_researchers",
    Base.metadata,
    Column(
        "project_id",
        ForeignKey("projects.project_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "researcher_id",
        ForeignKey("researchers.researcher_id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class Researcher(Base):
    __tablename__ = "researchers"

    researcher_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    organization: Mapped[str]
    email: Mapped[str]

    projects: Mapped[list["PublishedProject"]] = relationship(
        secondary=projects_researchers,
        back_populates="researchers",
        passive_deletes=True,
    )


class PublishedProject(Base):
    __tablename__ = "projects"

    project_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    published_date: Mapped[date]
    description: Mapped[Optional[str]]
    project_type: Mapped[Optional[str]]
    surveillance_status: Mapped[Optional[str]]
    citation: Mapped[Optional[str]]
    related_materials: Mapped[Optional[str]]
    project_publications: Mapped[Optional[str]]
    others_citing: Mapped[Optional[str]]

    researchers: Mapped[list["Researcher"]] = relationship(
        secondary=projects_researchers,
        back_populates="projects",
        cascade="all, delete",
    )

    datasets: Mapped[list["PublishedDataset"]] = relationship(
        "PublishedDataset",
        back_populates="project",
        cascade="all, delete",
        passive_deletes=True,
    )


class PublishedDataset(Base):
    __tablename__ = "datasets"

    dataset_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    earliest_date: Mapped[Optional[date]]
    latest_date: Mapped[Optional[date]]

    project_id: Mapped[str] = mapped_column(
        ForeignKey("projects.project_id", ondelete="CASCADE")
    )

    project: Mapped["PublishedProject"] = relationship(
        "PublishedProject",
        back_populates="datasets",
    )

    records: Mapped[list["PublishedRecord"]] = relationship(
        "PublishedRecord",
        back_populates="dataset",
        cascade="all, delete",
        passive_deletes=True,
    )


class PublishedRecord(Base):
    __tablename__ = "published_records"

    pharos_id: Mapped[str] = mapped_column(primary_key=True)
    dataset_id: Mapped[str] = mapped_column(
        ForeignKey("datasets.dataset_id", ondelete="CASCADE")
    )
    sample_id: Mapped[Optional[str]] = mapped_column(CoerceStr)
    animal_id: Mapped[Optional[str]] = mapped_column(CoerceStr)
    host_species: Mapped[str] = mapped_column(CoerceStr)
    host_species_ncbi_tax_id: Mapped[Optional[int]] = mapped_column(CoerceInt)
    spatial_uncertainty: Mapped[Optional[str]] = mapped_column(CoerceStr)
    collection_date: Mapped[date]
    collection_method_or_tissue: Mapped[Optional[str]] = mapped_column(CoerceStr)
    detection_method: Mapped[Optional[str]] = mapped_column(CoerceStr)
    primer_sequence: Mapped[Optional[str]] = mapped_column(CoerceStr)
    primer_citation: Mapped[Optional[str]] = mapped_column(CoerceStr)
    detection_target: Mapped[Optional[str]] = mapped_column(CoerceStr)
    detection_target_ncbi_tax_id: Mapped[Optional[int]] = mapped_column(CoerceInt)
    detection_outcome: Mapped[DetectionOutcome] = mapped_column(AliasDetectionOutcome)
    detection_measurement: Mapped[Optional[str]] = mapped_column(CoerceStr)
    detection_measurement_units: Mapped[Optional[str]] = mapped_column(CoerceStr)
    pathogen: Mapped[Optional[str]] = mapped_column(CoerceStr)
    pathogen_ncbi_tax_id: Mapped[Optional[int]] = mapped_column(CoerceInt)
    genbank_accession: Mapped[Optional[str]] = mapped_column(CoerceStr)
    detection_comments: Mapped[Optional[str]] = mapped_column(CoerceStr)
    organism_sex: Mapped[Optional[OrganismSex]] = mapped_column(AliasOrganismSex)
    dead_or_alive: Mapped[Optional[DeadOrAlive]] = mapped_column(AliasDeadOrAlive)
    health_notes: Mapped[Optional[str]] = mapped_column(CoerceStr)
    life_stage: Mapped[Optional[str]] = mapped_column(CoerceStr)
    age: Mapped[Optional[float]] = mapped_column(CoerceFloat)
    mass: Mapped[Optional[float]] = mapped_column(CoerceFloat)
    length: Mapped[Optional[float]] = mapped_column(CoerceFloat)
    geom: Mapped[WKTElement] = mapped_column(Geometry("POINT", srid=4326))

    dataset: Mapped["PublishedDataset"] = relationship(
        "PublishedDataset",
        back_populates="records",
    )
