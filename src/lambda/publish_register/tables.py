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


class ResearcherRecords(Base):
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
    pool = Column(Boolean)
    animal_id = Column(String(32))
    collection_method = Column(String(50))
    detection_method = Column(String(50))
    detection_target = Column(String(50))
    detection_target_ncbi_tax_id = Column(String(8))
    pathogen = Column(String(50))
    pathogen_ncbi_tax_id = Column(String(8))
    detection_outcome = Column(String(12))
    location = Column(Geometry("Point"))
    precision = Column(Numeric(1, 0))  # 1 meter scale
    collection_date = Column(Date())
    animal_identifier = Column(String(40))
    host_species = Column(String(50))
    host_ncbi_tax_id = Column(String(8))
    host_uncertanity = Column(String(15))
    life_stage = Column(String(15))
    sex = Column(String(1))  # M, F, U
    alive = Column(String(7))  # Y, N, U
    age = Column(BigInteger())  # age units seconds 100 years ==> integer of 10 units
    mass = Column(Numeric(7, 6))  # mass units to kg 0.000000
    length = Column(Numeric(7, 6))  # length units to meters 0.000000


def create_records(pharosId, researcherId, record):

    pr, dt, rec = pharosId.split("-")

    research_record = ResearcherRecords(researcher_id=researcherId, record_id=pharosId)

    record_ = Records(
        pharos_id=pharosId,
        project_id=pr,
        dataset_id=dt,
        record_id=rec,
        test_id=record["Test ID"]["dataValue"],
        sample_id=record["Sample ID"]["dataValue"],
        pool=1,  # record["Pool"]["dataValue"],
        animal_id=record["Animal ID or nickname"]["dataValue"],
        collection_method=record["Collection method or tissue"]["dataValue"],
        detection_method=record["Detection method"]["dataValue"],
        detection_target=record["Detection target"]["dataValue"],
        detection_target_ncbi_tax_id=record["Detection target NCBI Tax ID"][
            "dataValue"
        ],
        pathogen=record["Pathogen"]["dataValue"],
        pathogen_ncbi_tax_id=record["Pathogen NCBI Tax ID"]["dataValue"],
        detection_outcome=record["Detection outcome"]["dataValue"],
        location=f"POINT({record['Longitude']['dataValue']} {record['Latitude']['dataValue']})",
        precision=1,  # record["Precision"]["dataValue"],
        collection_date=datetime.datetime.strptime(
            record["Collection date"]["dataValue"], "%d/%m/%Y"
        ).date(),  # datetime.datetime( f"{record["Year"]}/{record["Month"]}/{record["Day"]}", '%y/%m/%d')
        host_species=record["Host species"]["dataValue"],
        host_ncbi_tax_id=record["Host NCBI Tax ID"]["dataValue"],
        host_uncertanity="",  # record["Uncertaninity"]["dataValue"],
        life_stage=record["Life stage"]["dataValue"],
        sex=record["Organism sex"]["dataValue"],
        alive=record["Dead or alive"]["dataValue"],
        age=float(record["Age"]["dataValue"]),
        mass=float(record["Mass"]["dataValue"]),
        length=float(record["Length"]["dataValue"]),
    )

    return record_, research_record
