import datetime
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from sqlalchemy.types import Numeric, String, Date, Boolean, BigInteger
from geoalchemy2 import Geometry

Base = declarative_base()


class Researcher(Base):
    __tablename__ = "Researchers"
    researcher_id = Column(String(20), primary_key=True)
    first_name = Column(String(40))
    last_name = Column(String(40))


class ResearchersRecords(Base):
    __tablename__ = "ResearcherRecords"
    id_pk = Column(BigInteger(), primary_key=True, autoincrement=True)
    researcher_id = Column(String(20))
    record_id = Column(String(41))


class Records(Base):
    __tablename__ = "Records"
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
    spatial_uncertainity = Column(Numeric(1, 0))  # 1 meter scale
    collection_date = Column(Date())
    animal_identifier = Column(String(40))
    host_species = Column(String(50))
    host_ncbi_tax_id = Column(String(8))
    # host_uncertanity = Column(String(15))
    life_stage = Column(String(15))
    organism_sex = Column(String(1))  # M, F, U
    dead_or_alive = Column(String(7))  # Y, N, U
    age = Column(BigInteger())  # age units seconds 100 years ==> integer of 10 units
    mass = Column(Numeric(7, 6))  # mass units to kg 0.000000
    length = Column(Numeric(7, 6))  # length units to meters 0.000000
