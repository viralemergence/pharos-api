import os
import json
import boto3
import sqlalchemy
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

# from auth import check_auth
from format import format_response
from tables import Records, ResearcherRecords, create_records

DYNAMODB = boto3.resource("dynamodb")
PROJECTS_TABLE = DYNAMODB.Table(os.environ["PROJECTS_TABLE_NAME"])
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])

RDS = boto3.client("rds")
HOST = os.environ["HOST"]
PORT = os.environ["PORT"]
USERNAME = os.environ["USERNAME"]
DATABASE = os.environ["DATABASE"]
PASSWORD = os.environ["PASSWORD"]


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    # Retrieve datasets from project table
    try:

        project = PROJECTS_TABLE.get_item(Key={"projectID": post_data["projectID"]})
        datasets = project["datasetIDs"]

    except Exception as e:
        return format_response(403, e)

    # Retrieve meta from datasets and filter released
    try:
        for dataset_id in datasets:
            dataset_meta = DATASETS_TABLE.get_item(
                Key={"datasetID": dataset_id, "recordID": "_meta"}
            )
            if dataset_meta["releaseStatus"] != "Released":
                datasets.remove(dataset_id)

    except Exception as e:
        return format_response(403, e)

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
                pharosId = (
                    f"{post_data['projectID']}-{post_data['datasetID']}-{record_id}"
                )
                pharos_record, researcher_record = create_records(
                    pharosId, post_data["researcherID"], record
                )
                records.extend([pharos_record, researcher_record])

            session.add_all(records)
            session.commit()

        return format_response(200, "Works")

    except Exception as e:
        return format_response(403, e)
