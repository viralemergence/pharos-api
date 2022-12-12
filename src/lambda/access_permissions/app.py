import os
import boto3
from botocore.exceptions import ClientError
import sqlalchemy
from sqlalchemy.engine import URL
import cfnresponse

RDS = boto3.client("rds")
CF = boto3.client("cloudformation")

HOST = os.environ["HOST"]
PORT = os.environ["PORT"]
MASTER = os.environ["MASTER"]
PASSWORD = os.environ["PASSWORD"]
USERNAME = os.environ["USERNAME"]
DATABASE = os.environ["DATABASE"]
SECRET = os.environ["SECRET"]

create = {
    "user": f"""CREATE USER "{USERNAME}" WITH PASSWORD '{SECRET}';""",
    "database": f'CREATE DATABASE "{DATABASE}";',
    "permissions": f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE "{DATABASE}" TO "{USERNAME}";',
}

extensions = {
    "postgis": "CREATE EXTENSION postgis;",
    "cascade": "CREATE EXTENSION aws_s3 CASCADE;",
}

delete = {
    "database": f'DROP DATABASE "{DATABASE}";',
    "user": f'DROP USER "{USERNAME}";',
}


def handle_statements(connection, response_data, **statement: dict) -> None:
    for key, value in statement.items():
        try:
            connection.execute(value)
            response_data[key] = str(value)

        except Exception as e:
            response_data[key] = str(e)


def lambda_handler(event, context):

    response_data = {}

    master_url = URL.create(
        drivername="postgresql+psycopg2",
        host=HOST,
        username=MASTER,
        port=PORT,
        password=PASSWORD,
        query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
    )

    try:
        engine = sqlalchemy.create_engine(master_url)
        connection = engine.connect()
        connection.execute("commit")

    except Exception as e:
        response_data["mconnection"] = str(e)

    handle_statements(connection, response_data, **create)

    connection.close()

    database_url = URL.create(
        drivername="postgresql+psycopg2",
        host=HOST,
        database=DATABASE,
        username=MASTER,
        port=PORT,
        password=PASSWORD,
        query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
    )

    try:
        engine = sqlalchemy.create_engine(database_url)
        connection = engine.connect()
        connection.execute("commit")

    except Exception as e:
        response_data["dbconnection"] = str(e)

    handle_statements(connection, response_data, **extensions)

    connection.close()

    cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
