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


def database(parameters):
    database_url = f"postgresql://{parameters['username']}:{parameters['password']}@{parameters['host']}:{parameters['port']}/{parameters['database']}?sslmode=require"
    engine = sqlalchemy.create_engine(database_url)
    return engine


def lambda_handler(event, _):
    # Minimal working example
    post_data = json.loads(event.get("body", "{}"))

    today = datetime.date.today()
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
