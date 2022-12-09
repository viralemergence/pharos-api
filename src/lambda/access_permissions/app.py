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

# STACK = os.environ["STACK"]

create = [
    # f'CREATE DATABASE "{DATABASE}";',
    # 'CREATE EXTENSION postgis;',
    # 'CREATE EXTENSION aws_s3 CASCADE;',
    f'CREATE USER "{USERNAME}" WITH ENCRYPTED PASSWORD "{SECRET}";',
    f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE "{DATABASE}" TO "{USERNAME}";',
]

delete = [f'DROP DATABASE "{DATABASE}";', f'DROP USER "{USERNAME}"']


# def stack_status(status) -> bool:
#     stack_data = CF.describe_stacks(StackName=STACK)
#     return stack_data["Stacks"][0]["StackStatus"] == status


def lambda_handler(event, context):

    database_url = URL.create(
        drivername="postgresql+psycopg2",
        host=HOST,
        username=MASTER,
        port=PORT,
        password=PASSWORD,
        query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
    )

    responseData = {}

    try:

        engine = sqlalchemy.create_engine(database_url)

        connection = engine.connect()
        connection.execute("commit")

        # if stack_status("CREATE_IN_PROGRESS"):
        for statement in create:
            connection.execute(statement)

        # if stack_status("DELETE_IN_PROGRESS"):
        #     for statement in delete:
        #         connection.execute(statement)

        connection.close()

        responseData["User"] = USERNAME
        responseData["Database"] = DATABASE

    except Exception as e:
        print(e)
        #responseData["Error"] = "Failed"
        #cfnresponse.send(event, context, cfnresponse.FAILED, responseData)

    cfnresponse.send(event, context, cfnresponse.SUCCESS, responseData)