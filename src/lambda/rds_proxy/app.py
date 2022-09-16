import os
import json
import boto3
import sqlalchemy
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker

# from auth import check_auth
from format import format_response
from tables import TestTable, AnimalTable


RDS = boto3.client("rds")
HOST = os.environ["HOST"]
PORT = os.environ["PORT"]
USERNAME = os.environ["USERNAME"]
DATABASE = os.environ["DATABASE"]


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    try:

        database_token = RDS.generate_db_auth_token(
            DBHostname=HOST,
            Port=PORT,
            DBUsername=USERNAME,
        )

        database_url = URL.create(
            drivername="postgresql+psycopg2",
            host=HOST,
            database=DATABASE,
            username=USERNAME,
            port=PORT,
            password=database_token,
            query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
        )

        engine = sqlalchemy.create_engine(database_url)

        Session = sessionmaker(bind=engine)
        session = Session()

        session.add()  # Here use post_data
        session.commit()

        session.close()
        return format_response(200, "Works")

    except Exception as e:
        return format_response(403, e)
