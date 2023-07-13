import os
import json
import boto3
import sqlalchemy
from sqlalchemy import URL


DATABASE = os.environ["DATABASE"]

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")
response = SECRETS_MANAGER.get_secret_value(SecretId="data-lab-rds-test")
CREDENTIALS = json.loads(response["SecretString"])


def get_engine():

    database_url = URL.create(
        drivername="postgresql+psycopg2",
        # host=CREDENTIALS["host"],
        host="data-lab-rds-proxy.proxy-cvsrrvlopzxr.us-west-1.rds.amazonaws.com",
        database=DATABASE,
        username=CREDENTIALS["username"],
        # port=CREDENTIALS["port"],
        port=5432,
        password=CREDENTIALS["password"],
        query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
    )

    engine = sqlalchemy.create_engine(database_url)

    return engine
