import os
import json
import boto3
import sqlalchemy
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

# from auth import check_auth
from format import format_response
from tables import Records, ResearcherRecords, create_record


RDS = boto3.client("rds")
HOST = os.environ["HOST"]
PORT = os.environ["PORT"]
USERNAME = os.environ["USERNAME"]
DATABASE = os.environ["DATABASE"]
PASSWORD = os.environ["PASSWORD"]


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    try:

        database_url = URL.create(
            drivername="postgresql+psycopg2",
            host=HOST,
            database=DATABASE,
            username=USERNAME,
            port=PORT,
            password=PASSWORD,
            query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
        )

        engine = sqlalchemy.create_engine(database_url)

        Session = sessionmaker(bind=engine)

        Records.__table__.create(engine, checkfirst=True)
        ResearcherRecords.__table__.create(engine, checkfirst=True)

        with Session() as session:

            records = []

            for record_id, record in post_data["register"].items():
                id_ = f"{post_data['projectID']}-{post_data['datasetID']}-{record_id}"
                pharos_record, researcher_record = create_record(
                    post_data["researcherID"], id_, record
                )
                records.extend([pharos_record, researcher_record])

            session.add_all(records)
            session.commit()

        return format_response(200, "Works")

    except Exception as e:
        return format_response(403, e)
