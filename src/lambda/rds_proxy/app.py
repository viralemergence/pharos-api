import os
import json
import datetime
import boto3
import sqlalchemy
from sqlalchemy import insert
from sqlalchemy.orm import sessionmaker, scoped_session

# from auth import check_auth
from format import format_response
from tables import TestTable, AnimalTable

DYNAMODB = boto3.resource("dynamodb")
USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])
RDS = boto3.client("rds")


# class DbSession:
#     def __init__(self, engine):
#         self.engine = engine

#     def __enter__(self):
#         self.connection = (
#             self.engine.connect()
#         )  # // Reuse the engine instead of creating a new one
#         self.session = scoped_session(
#             sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
#         )
#         return self.session

#     def __exit__(self, type, value, traceback):
#         self.session.commit()
#         self.session.close()
#         self.connection.close()


# This function might need to be async as per AWS documentation:
# You must ensure that you have static or previously resolved credentials if you call this method synchronously (with no callback),
# # otherwise it may not properly sign the request. If you cannot guarantee this (you are using an asynchronous credential provider,
# # i.e., EC2 IAM roles), you should always call this method with an asynchronous callback.

# IAM authentication is required, hence IAM token must be created
# def proxy_token(parameters):
#     token = RDS.generate_db_auth_token(
#         DBHostname=parameters["host"],  # RDS proxy endpoint
#         Port=parameters["port"],  # RDS proxy endpoint
#         DBUsername=parameters["username"],  # DB schema user
#     )
#     return token

# Set secretmanager in the meantime.


def database(parameters):
    # token = proxy_token(parameters) - TODO
    database_url = f"postgresql://{parameters['username']}:{parameters['password']}@{parameters['host']}:{parameters['port']}/{parameters['database']}?sslmode=require"
    engine = sqlalchemy.create_engine(database_url)
    return engine


def unpack():
    pass


def lambda_handler(event, _):
    post_data = json.loads(event.get("body", "{}"))

    today = datetime.date.today()
    # d1 = today.strftime("%d/%m/%Y")

    try:
        engine = database(post_data["parameters"])

        Session = sessionmaker(bind=engine)
        session = Session()

        session.add(
            TestTable(
                record_id="test",
                test_id="test",
                sample_id="test",
                animal_id="test",
                collection_method="test",
                detection_method="test",
                detection_target="test",
                detection_target_ncbi_tax_id="test",
                detection_pathogen="test",
                detection_pathogen_ncbi_tax_id="test",
                detection_measurement=3.1416,
                genebank_accession="test",
                test_specifications="this is a test",
                detection_outcome="P",
                location="POINT(-33.9034 152.73457)",
                collection_date=today,
            )
        )

        session.commit()
        session.close()

        return format_response(200, "Works")

    except Exception as e:
        return format_response(403, e)


# def lambda_handler(event, _):

#     post_data = json.loads(event.get("body", "{}"))
#     # Authentication doesn't work, possibly because dynabodb tables are not in same VPC and VPC security group - TODO

#     try:
#         engine = database(post_data["parameters"])

#         with engine.connect() as connection:
#             # Something along the lines of...

#             for row in post_data["data"]:
#                 test, animal = unpack()
#                 test_query = insert()
#                 animal_query = insert()
#                 test_result = connection.execute(test_query)
#                 animal_result = connection.execute(animal_query)

#         return format_response(200, connection)

#     except Exception as e:
#         return format_response(403, e)
