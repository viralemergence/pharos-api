from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from sqlalchemy.types import Numeric, String, Text, Date
from geoalchemy2 import Geometry

# import postgis


Base = declarative_base()


class TestTable(Base):
    __tablename__ = "tests"
    record_id = Column(String(32), primary_key=True)  # Primary Key - pharos unique id
    test_id = Column(String(25))
    sample_id = Column(String(25))
    animal_id = Column(String(32))  # Foreign Key - pharos unique id
    collection_method = Column(String(50))  # more?
    detection_method = Column(String(50))
    detection_target = Column(String(50))
    detection_target_ncbi_tax_id = Column(String(10))
    detection_pathogen = Column(String(50))
    detection_pathogen_ncbi_tax_id = Column(String(10))
    detection_measurement = Column(Numeric())  # TBD (link to equipment output?)
    genebank_accession = Column(String(10))
    test_specifications = Column(Text())
    detection_outcome = Column(String(1))  # P positive, N negative, I inconclusive
    location = Column(Geometry("Point"))  # Point or area
    collection_date = Column(Date())

    def __repr__(self):
        return "<TestTable(record.id=self.record_id, \
            test.id=self.test_id, \
            sample.id=self.sample_id, \
            animal.id=self.animal_id, \
            collection.method=self.collection_method, \
            detection.method=self.detection_method, \
            detection.target=self.detection_target, \
            detection.target.ncbi.tax.id=self.detection_target_ncbi_tax_id, \
            detection.pathogen=self.detection_pathogen, \
            detection.pathogen.ncbi.tax.id=self.detection_pathogen_ncbi_tax_id, \
            detection.measurement=self.detection_measurement, \
            genebank.accession=self.genebank_accession, \
            test_specifications=self.test_specifications, \
            detection.outcome=self.detection_outcome, \
            location=self.location, \
            collection.date=self.collection_date)>"


class AnimalTable(Base):
    __tablename__ = "animals"
    animal_id = Column(String(32), primary_key=True)  # Primary Key - pharos unique id
    animal_identifier = Column(String(40))
    host_species = Column(String(50))  # Latin name
    host_ncbi_tax_id = Column(String(10))
    life_stage = Column(
        String(2)
    )  # TBD NN neo-natal, JU juvenile, SA sub-adult, AD adult,
    sex = Column(String(1))  # M male, F female, U unknown
    alive = Column(String(1))  # Y yes, N no, U unknown
    age = Column(Numeric())
    mass = Column(Numeric())
    length = Column(Numeric())
    health_notes = Column(Text())

    def __repr__(self):
        return "<AnimalTable(animal.id=self.animal_id, \
            animal.identifier=self.animal_identifier, \
            host.species=self.host_species, \
            host.ncbi.tax.id=self.host_ncbi_tax_id, \
            life.stage=self.life_stage, \
            sex=self.sex, \
            alive=self.alive, \
            age=self.age, \
            mass=self.mass, \
            length=self.length, \
            health.notes=self.health_notes)>"


# Base.metadata.create_all(engine)
