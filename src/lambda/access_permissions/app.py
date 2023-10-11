import os
import json

import boto3

# from botocore.exceptions import ClientError
import sqlalchemy
from sqlalchemy.engine import URL, Connection
import cfnresponse

from models import Base

print("set up boto3 clients")
RDS = boto3.client("rds")
CF = boto3.client("cloudformation")
SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-east-2")

print("get connection info")
HOST = os.environ["HOST"]
PORT = int(os.environ["PORT"])
DATABASE = os.environ["DATABASE"]
# USERNAME = os.environ["USERNAME"]

extensions = {}


print("declare get_secret")


def get_secret(secret_id):
    print("get_secret called")
    response = SECRETS_MANAGER.get_secret_value(SecretId=secret_id)
    print("secret response:", response)
    return json.loads(response["SecretString"])


def handle_statements(
    connection: Connection,
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

    # Make the custom resource handle delete events.
    # At this point, we're not making this clean up the
    # database so that we don't unintentionally delete
    # them, they'll need to be cleaned up manually.
    if event["RequestType"] == "Delete":
        print("Handle Delete Event")
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
        return

    credentials = get_secret("pharos-database-DBAdminSecret")

    response_data: dict[str, str] = {}

    print("Connect to DB as Superuser")
    master_url = URL.create(
        drivername="postgresql+psycopg2",
        host=HOST,
        username=credentials["username"],
        port=PORT,
        password=credentials["password"],
        query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
        database="postgres",
    )

    try:
        engine = sqlalchemy.create_engine(master_url)
        connection = engine.connect()
        connection.execute(sqlalchemy.sql.text("COMMIT"))

    except Exception as e:  # pylint: disable=broad-except
        response_data["mconnection"] = str(e)
        cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
        return

        # try:
        #     print("Check if secret exists")
        #     SECRETS_MANAGER.get_secret_value(SecretId=DATABASE)

        #     cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
        #     return

        # except ClientError:
        #     print("Create random password")
        #     response = SECRETS_MANAGER.get_random_password(
        #         ExcludePunctuation=True, IncludeSpace=False
        #     )
        #     new_password = response["RandomPassword"]

        #     print("Store new secret")
        #     response = SECRETS_MANAGER.create_secret(
        #         Name=DATABASE,
        #         Description=f'Username and Password for database "{DATABASE}" used for Pharos',
        #         SecretString=(
        #             json.dumps(
        #                 {
        #                     "username": USERNAME,
        #                     "password": new_password,
        #                     "host": HOST,
        #                     "port": PORT,
        #                 }
        #             )
        #         ),
        #         Tags=[
        #             {"Key": "Project", "Value": "Pharos"},
        #             {"Key": "Project:Detail", "Value": "Pharos"},
        #         ],
        #     )

        # print("Create database, user, and grant permissions")
        # handle_statements(
        #     connection,
        #     response_data,
        #     {
    #         "database": f'CREATE DATABASE "{DATABASE}";',
    #         "user": f"""CREATE USER "{USERNAME}" WITH PASSWORD '{new_password}';""",
    #         "priviliges": f"""GRANT ALL PRIVILEGES ON DATABASE "{DATABASE}" TO "{USERNAME}";""",
    #     },
    # )

    print("Create database")
    handle_statements(
        connection,
        response_data,
        {
            "database": f'CREATE DATABASE "{DATABASE}";',
        },
    )

    connection.close()

    print("Connect to new database as superuser")
    database_url = URL.create(
        drivername="postgresql+psycopg2",
        host=HOST,
        database=DATABASE,
        username=credentials["username"],
        port=PORT,
        password=credentials["password"],
        query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
    )

    try:
        engine = sqlalchemy.create_engine(database_url)
        connection = engine.connect()
        connection.execute(sqlalchemy.sql.text("COMMIT"))

        print("Install postgis and aws_s3 extensions")
        handle_statements(
            connection,
            response_data,
            {
                "postgis": "CREATE EXTENSION postgis;",
                "cascade": "CREATE EXTENSION aws_s3 CASCADE;",
            },
        )

        print("Create tables")
        Base.metadata.create_all(engine)

        connection.close()

    except Exception as e:  # pylint: disable=broad-except
        response_data["dbconnection"] = str(e)

    cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data)
