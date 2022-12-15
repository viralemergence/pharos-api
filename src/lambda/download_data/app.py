import os
import json
import datetime
import boto3
import sqlalchemy
from sqlalchemy.engine import URL
from auth import check_auth
from format import format_response

RDS = boto3.client("rds")
SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")

DATABASE = os.environ["DATABASE"]
REGION = os.environ["REGION"]
BUCKET = os.environ["BUCKET"]

response = SECRETS_MANAGER.get_secret_value(SecretId=DATABASE)
CREDENTIALS = json.loads(response["SecretString"])


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    authorized = check_auth(post_data["researcherID"])

    if not authorized:
        return format_response(403, "Not Authorized")

    try:

        database_url = URL.create(
            drivername="postgresql+psycopg2",
            host=CREDENTIALS["host"],
            database=DATABASE,
            username=CREDENTIALS["username"],
            port=CREDENTIALS["port"],
            password=CREDENTIALS["password"],
            query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
        )

        engine = sqlalchemy.create_engine(database_url)

        connection = engine.connect()

        date = datetime.datetime.utcnow().date()

        file_path = f"data_{datetime.datetime.strftime(date, '%Y_%m_%d')}"

        s3_uri = f"aws_commons.create_s3_uri('{BUCKET}', '{file_path}', '{REGION}')"

        connection.execute(
            f"SELECT * from aws_s3.query_export_to_s3('SELECT * FROM records', {s3_uri}, options :='format csv');"
        )

        connection.close()

        return format_response(200, "Data downloaded")

    except Exception as e:
        return format_response(403, e)
