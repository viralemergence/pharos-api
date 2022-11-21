from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from sqlalchemy.types import Numeric, String, Date, Boolean, BigInteger, TypeDecorator
from geoalchemy2 import Geometry


class StringToNumeric(TypeDecorator):
    """Typecast string to numeric."""

    impl = Numeric

    def __init__(self, *arg, **kw):
        TypeDecorator.__init__(self, *arg, **kw)

    def process_bind_param(self, value, dialect):
        return float(value)


class StringToInteger(TypeDecorator):
    """Typecast string to integer."""

    impl = BigInteger

    def process_bind_param(self, value, dialect):
        return int(value)


def declarative_constructor(self, **kwargs):
    """Don't raise a TypeError for unknown attribute names."""
    attribute_type = type(self)
    for k in kwargs:
        if not hasattr(attribute_type, k):
            continue
        setattr(self, k, kwargs[k])


Base = declarative_base(constructor=declarative_constructor)


class Researchers(Base):
    __tablename__ = "researchers"
    researcher_id = Column(String(20), primary_key=True)
    first_name = Column(String(40))
    last_name = Column(String(40))


class ResearchersRecords(Base):
    __tablename__ = "researchersrecords"
    id_pk = Column(BigInteger(), primary_key=True, autoincrement=True)
    researcher_id = Column(String(20))
    record_id = Column(String(41))


class Records(Base):
    __tablename__ = "records"
    pharos_id = Column(String(41), primary_key=True)  # compund key proj-set-rec
    project_id = Column(String(20))
    dataset_id = Column(String(20))
    record_id = Column(String(20))
    test_id = Column(String(25))
    sample_id = Column(String(25))
    # pool = Column(Boolean)
    animal_id = Column(String(32))
    collection_method = Column(String(50))
    detection_method = Column(String(50))
    detection_target = Column(String(50))
    detection_target_ncbi_tax_id = Column(String(8))
    pathogen = Column(String(50))
    pathogen_ncbi_tax_id = Column(String(8))
    detection_outcome = Column(String(12))
    location = Column(Geometry("Point"))
    spatial_uncertainity = Column(StringToNumeric(1, 0))  # 1 meter scale
    collection_date = Column(Date())
    animal_identifier = Column(String(40))
    host_species = Column(String(50))
    host_ncbi_tax_id = Column(String(8))
    # host_uncertanity = Column(String(15))
    life_stage = Column(String(15))
    organism_sex = Column(String(1))  # M, F, U
    dead_or_alive = Column(String(7))  # Y, N, U
    age = Column(
        StringToInteger()
    )  # age units seconds 100 years ==> integer of 10 units
    mass = Column(StringToNumeric(7, 6))  # mass units to kg 0.000000
    length = Column(StringToNumeric(7, 6))  # length units to meters 0.000000
