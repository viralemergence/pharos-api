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

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]

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
        datasets = project["Item"]["datasetIDs"]

    except Exception as e:
        return format_response(403, e)

    # Retrieve meta from datasets and filter released
    try:
        for dataset_id in datasets:
            dataset_meta = DATASETS_TABLE.get_item(
                Key={"datasetID": dataset_id, "recordID": "_meta"}
            )
            if dataset_meta["Item"]["releaseStatus"] != "Released":
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

            for dataset in datasets:
                # Retrieve last version of the register
                key_list = S3CLIENT.list_objects_v2(
                    Bucket=DATASETS_S3_BUCKET, Prefix=f"{dataset}/"
                )["Contents"]

                key_list.sort(key=lambda item: item["LastModified"], reverse=True)
                key = key_list[0]["Key"]

                register_response = S3CLIENT.get_object(
                    Bucket=DATASETS_S3_BUCKET, Key=key
                )
                register = register_response["Body"].read().decode("UTF-8")

                register = json.loads(register)

                # Create records
                for record_id, record in register.items():
                    pharosId = f"{post_data['projectID']}-{dataset}-{record_id}"
                    pharos_record, researcher_record = create_records(
                        pharosId, post_data["researcherID"], record
                    )
                    records.extend([pharos_record, researcher_record])

            # Upload records
            session.add_all(records)
            session.commit()

        return format_response(200, "Works")

    except Exception as e:
        return format_response(403, e)
