import os
import json

import boto3
from botocore.exceptions import ClientError
import sqlalchemy
from sqlalchemy.engine import URL
import cfnresponse

RDS = boto3.client("rds")
CF = boto3.client("cloudformation")
SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")

HOST = os.environ["HOST"]
PORT = os.environ["PORT"]
DATABASE = os.environ["DATABASE"]
USERNAME = os.environ["USERNAME"]

extensions = {}

# delete = {
#     "database": f'DROP DATABASE "{DATABASE}";',
#     "user": f'DROP USER "{USERNAME}";',
# }


def get_secret(secret_id):
    response = SECRETS_MANAGER.get_secret_value(SecretId=secret_id)
    return json.loads(response["SecretString"])


# Requesting secret at during init so it's cached in the runtime
CREDENTIALS = get_secret("data-lab-rds-test")


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
        username=CREDENTIALS["username"],
        port=PORT,
        password=CREDENTIALS["password"],
        query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
    )

    try:
        engine = sqlalchemy.create_engine(master_url)
        connection = engine.connect()
        connection.execute("commit")

    except Exception as e:
        response_data["mconnection"] = str(e)

    response = SECRETS_MANAGER.get_random_password()
    new_password = response["RandomPassword"]

    response = SECRETS_MANAGER.create_secret(
        Name=DATABASE,
        Description=f'Username and Password for database "{DATABASE}" used for Pharos',
        SecretString=(
            f'{{"username":"{USERNAME}",'
            f'"password":"{new_password}",'
            f'"host":{HOST},'
            f'"port":{PORT}}}'
        ),
        Tags=[
            {"Key": "Project", "Value": "Pharos"},
            {"Key": "Project:Detail", "Value": "Pharos"},
        ],
    )

    handle_statements(
        connection,
        response_data,
        **{
            "database": f'CREATE DATABASE "{DATABASE}";',
            "user": f"""CREATE USER "{USERNAME}" WITH PASSWORD '{new_password}';""",
            "priviliges": f"""GRANT ALL PRIVILEGES ON DATABASE "{DATABASE}" TO "{USERNAME}";""",
        },
    )

    connection.close()

    database_url = URL.create(
        drivername="postgresql+psycopg2",
        host=HOST,
        database=DATABASE,
        username=CREDENTIALS["username"],
        port=PORT,
        password=CREDENTIALS["password"],
        query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
    )

    try:
        engine = sqlalchemy.create_engine(database_url)
        connection = engine.connect()
        connection.execute("commit")

    except Exception as e:
        response_data["dbconnection"] = str(e)

    handle_statements(
        connection,
        response_data,
        **{
            "postgis": "CREATE EXTENSION postgis;",
            "cascade": "CREATE EXTENSION aws_s3 CASCADE;",
        },
    )

    connection.close()

    cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
