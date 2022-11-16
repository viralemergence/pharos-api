import os
import json
import datetime
import boto3
import sqlalchemy
from sqlalchemy.engine import URL
from auth import check_auth
from format import format_response

RDS = boto3.client("rds")
HOST = os.environ["HOST"]
PORT = os.environ["PORT"]
USERNAME = os.environ["USERNAME"]
DATABASE = os.environ["DATABASE"]
PASSWORD = os.environ["PASSWORD"]
REGION = os.environ["REGION"]
BUCKET = os.environ["BUCKET"]


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    authorized = check_auth(post_data["researcherID"])

    if not authorized:
        return format_response(403, "Not Authorized")

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

        connection = engine.connect()

        date = datetime.datetime.utcnow().date()

        file_path = f"data_{datetime.datetime.strftime(date, '%Y_%m_%d')}"

        s3_uri = f"aws_commons.create_s3_uri('{BUCKET}', '{file_path}', '{REGION}')"

        connection.execute(  # TODO
            f"SELECT * from aws_s3.query_export_to_s3('SELECT * FROM Records', {s3_uri}, options :='format csv);"
        )

        connection.close()

        return format_response(200, "Data downloaded")

    except Exception as e:
        return format_response(403, e)
