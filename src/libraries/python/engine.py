import os
import json
import boto3
import sqlalchemy
from sqlalchemy import URL


DATABASE = os.environ["DATABASE"]

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-east-2")
response = SECRETS_MANAGER.get_secret_value(SecretId="pharos-database-DBAdminSecret")
CREDENTIALS = json.loads(response["SecretString"])


def get_engine():

    database_url = URL.create(
        drivername="postgresql+psycopg2",
        # host=CREDENTIALS["host"],
        host="pharos-database-proxy.proxy-c3ngc0ulwwgm.us-east-2.rds.amazonaws.com",
        database=DATABASE,
        username=CREDENTIALS["username"],
        # port=CREDENTIALS["port"],
        port=5432,
        password=CREDENTIALS["password"],
        query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
    )

    engine = sqlalchemy.create_engine(database_url)

    return engine
