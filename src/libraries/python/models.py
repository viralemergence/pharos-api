# from sqlalchemy.orm import declarative_base
# from sqlalchemy import Column
# from sqlalchemy.types import Numeric, String, Date, Boolean, BigInteger, TypeDecorator
# from geoalchemy2 import Geometry


# class StringToNumeric(TypeDecorator):
#     """Typecast string to numeric."""

#     impl = Numeric

#     def __init__(self, *arg, **kw):
#         TypeDecorator.__init__(self, *arg, **kw)

#     def process_bind_param(self, value, dialect):
#         if value is None:
#             return value
#         return float(value)


# class StringToInteger(TypeDecorator):
#     """Typecast string to integer."""

#     impl = BigInteger

#     def process_bind_param(self, value, dialect):
#         if value is None:
#             return value
#         return int(value)


# def declarative_constructor(self, **kwargs):
#     """Don't raise a TypeError for unknown attribute names."""
#     attribute_type = type(self)
#     for k in kwargs:
#         if not hasattr(attribute_type, k):
#             continue
#         setattr(self, k, kwargs[k])


# Base = declarative_base(constructor=declarative_constructor)


# class Researchers(Base):
#     __tablename__ = "researchers"
#     researcher_id = Column(String(20), primary_key=True)
#     first_name = Column(String(40))
#     last_name = Column(String(40))


# class ResearchersTests(Base):
#     __tablename__ = "researcherstests"
#     id_pk = Column(BigInteger(), primary_key=True, autoincrement=True)
#     researcher_id = Column(String(20))
#     record_id = Column(String(41))


# class Tests(Base):
#     __tablename__ = "tests"
#     pharos_id = Column(String(41), primary_key=True)  # compund key proj-set-rec
#     project_id = Column(String(20))
#     dataset_id = Column(String(20))
#     record_id = Column(String(20))
#     # row_id = Column(String(25))
#     sample_id = Column(String(25), default=None)
#     animal_id = Column(String(32), default=None)
#     host_species = Column(String(50))
#     host_ncbi_tax_id = Column(String(8), default=None)
#     location = Column(Geometry("Point"))  # latitude, longitude
#     spatial_uncertainty = Column(
#         StringToNumeric(1, 0), default=None
#     )  # 1 meter scale / spatial uncertainity and spatial uncertainity units
#     collection_date = Column(
#         Date()
#     )  # collection day, collection month, collection year
#     collection_method_or_tissue = Column(String(50), default=None)
#     detection_method = Column(String(50), default=None)
#     # primer_sequence
#     # primer_citation
#     detection_target = Column(String(50), default=None)
#     detection_target_ncbi_tax_id = Column(String(8), default=None)
#     detection_outcome = Column(String(12), default=None)
#     # detection_measurement
#     # detection_measurement_units
#     pathogen = Column(String(50))
#     pathogen_ncbi_tax_id = Column(String(8), default=None)
#     # genbank_accession
#     # detection_comments
#     organism_sex = Column(String(1), default=None)  # M, F, U
#     dead_or_alive = Column(String(7), default=None)  # Y, N, U
#     # health_notes
#     life_stage = Column(String(15), default=None)
#     age = Column(
#         StringToInteger(), default=None
#     )  # age units seconds 100 years ==> integer of 10 digits / age, age units
#     mass = Column(
#         StringToNumeric(12, 6), default=None
#     )  # mass units to kg xxxxxx.xxxxxx / mass, mass units / tons - mg
#     length = Column(
#         StringToNumeric(8, 6), default=None
#     )  # length units to meters xx.xxxxxx / length, length units / m - mm
#     # pool = Column(Boolean)
#     # host_uncertanity = Column(String(15))
