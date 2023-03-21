# pylint: disable-all

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
PORT = int(os.environ["PORT"])
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


def handle_statements(
    connection: sqlalchemy.Connection,
    response_data: dict[str, str],
    statements: dict[str, str],
) -> None:
    for key, statement in statements.items():
        try:
            connection.execute(sqlalchemy.sql.text(statement))
            response_data[key] = str(statement)

        except Exception as e:  # pylint: disable=broad-except
            response_data[key] = str(e)


def lambda_handler(event, context):
    # Printing event in case the custom resource
    # needs to be manually deleted or marked as success
    print("EVENT:")
    print(event)

    # Make it handle delete events instantly
    # TODO: Add cleanup here...

    response_data: dict[str, str] = {}

    if event["RequestType"] == "Delete":
        print("Handle Delete Event")
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
        return

    print("Connect to DB as Superuser")
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
        connection.execute(sqlalchemy.sql.text("COMMIT"))

    except Exception as e:
        response_data["mconnection"] = str(e)
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
        return

    try:
        print("Check if secret exists")
        SECRETS_MANAGER.get_secret_value(SecretId=DATABASE)
        # if secret exists, short-circuit setup
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
        return

    except ClientError:
        print("Create random password")
        response = SECRETS_MANAGER.get_random_password(
            ExcludePunctuation=True, IncludeSpace=False
        )
        new_password = response["RandomPassword"]

        print("Store new secret")
        response = SECRETS_MANAGER.create_secret(
            Name=DATABASE,
            Description=f'Username and Password for database "{DATABASE}" used for Pharos',
            SecretString=(
                json.dumps(
                    {
                        "username": USERNAME,
                        "password": new_password,
                        "host": HOST,
                        "port": PORT,
                    }
                )
            ),
            Tags=[
                {"Key": "Project", "Value": "Pharos"},
                {"Key": "Project:Detail", "Value": "Pharos"},
            ],
        )

        print("Create database, user, and grant permissions")
        handle_statements(
            connection,
            response_data,
            {
                "database": f'CREATE DATABASE "{DATABASE}";',
                "user": f"""CREATE USER "{USERNAME}" WITH PASSWORD '{new_password}';""",
                "priviliges": f"""GRANT ALL PRIVILEGES ON DATABASE "{DATABASE}" TO "{USERNAME}";""",
            },
        )

        connection.close()

        print("Connect to new database as superuser")
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
            connection.execute(sqlalchemy.sql.text("COMMIT"))

        except Exception as e:
            response_data["dbconnection"] = str(e)

        print("Install postgis and aws_s3 extensions")
        handle_statements(
            connection,
            response_data,
            {
                "postgis": "CREATE EXTENSION postgis;",
                "cascade": "CREATE EXTENSION aws_s3 CASCADE;",
            },
        )

        connection.close()

        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
