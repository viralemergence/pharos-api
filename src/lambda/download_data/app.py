import os

import boto3
from auth import check_auth
from format import format_response
from pydantic import BaseModel, ValidationError
from register import User

LAMBDACLIENT = boto3.client("lambda")
CREATE_CSV_EXPORT_LAMBDA = os.environ["CREATE_CSV_EXPORT_LAMBDA"]

# DYNAMODB = boto3.resource("dynamodb")
# METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

# S3CLIENT = boto3.client("s3")
# DATA_DOWNLOAD_BUCKET_NAME = os.environ["DATA_DOWNLOAD_BUCKET_NAME"]

# DATABASE = os.environ["DATABASE"]
# SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-east-2")
# response = SECRETS_MANAGER.get_secret_value(SecretId="pharos-database-DBAdminSecret")
# CREDENTIALS = json.loads(response["SecretString"])


class CreateExportData(BaseModel):
    """event payload to export a csv of published records"""

    user: User


def lambda_handler(event, _):

    try:
        user = check_auth(event)
    except ValidationError:
        return format_response(403, "Not Authorized")

    if not user:
        return format_response(403, "Not Authorized")

    LAMBDACLIENT.invoke(
        FunctionName=CREATE_CSV_EXPORT_LAMBDA,
        InvocationType="Event",
        Payload=CreateExportData(user=user).json(by_alias=True),
    )

    return format_response(200, "Data export started")

    # if not user.download_ids:
    #     user.download_ids = set()

    # METADATA_TABLE.put_item(Item=user.table_item())

    # date = datetime.utcnow().date()
    # file_path = f"data_{date.strftime('%Y_%m_%d')}"
    # s3_uri = f"aws_commons.create_s3_uri('{DATA_DOWNLOAD_BUCKET_NAME}', '{file_path}', '{REGION}')"

    # engine = get_engine()
    # engine.execution_options(isolation_level="AUTOCOMMIT")

    # connection = engine.connect()
    # start = time.time()
    # connection.execute(
    #     sql.text(
    #         "SELECT pg_sleep(10);"
    #         + f"SELECT * from aws_s3.query_export_to_s3('SELECT * FROM published_records', {s3_uri}, options :='format csv, header');"
    #     )
    # )
    # end = time.time()
    # print(f"Query took {end - start} seconds")
    # connection.close()

    # start = time.time()
    # with Session(engine) as session:
    #     session.execute(
    #         sql.text(
    #             f"SELECT * from aws_s3.query_export_to_s3('SELECT * FROM published_records', {s3_uri}, options :='format csv, header');"
    #         )
    #     )
    # end = time.time()
    # print(f"Query took {end - start} seconds")

    # connection = psycopg2.connect(
    #     database=DATABASE,
    #     user=CREDENTIALS["username"],
    #     password=CREDENTIALS["password"],
    #     host="pharos-database-proxy.proxy-c3ngc0ulwwgm.us-east-2.rds.amazonaws.com",
    #     port=5432,
    #     async_=1,
    # )

    # # connection.set_session(autocommit=True)

    # cursor = connection.cursor()

    # start = time.time()
    # cursor.execute(
    #     "SELECT pg_sleep(10);"
    #     + f"SELECT * from aws_s3.query_export_to_s3('SELECT * FROM published_records', {s3_uri}, options :='format csv, header');"
    # )
    # end = time.time()
    # print(f"Query took {end - start} seconds")

    # cursor.close()
    # connection.close()

    return format_response(200, {"uri": s3_uri})

    # engine = get_engine()
    # with Session(engine) as session:
    #     print("S3 URI: ")
    #     print(s3_uri)

    #     session.execute(
    #         sql.text(
    #             f"SELECT * from aws_s3.query_export_to_s3('SELECT * FROM published_records', {s3_uri}, options :='format csv, header');"
    #         )
    #     )

    #     return format_response(200, {"uri": s3_uri})


# pylint: disable-all

# import os
# import json
# import datetime
# import boto3
# import sqlalchemy
# from sqlalchemy.engine import URL
# from auth import check_auth
# from format import format_response

# RDS = boto3.client("rds")
# SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")

# DATABASE = os.environ["DATABASE"]
# REGION = os.environ["REGION"]
# BUCKET = os.environ["BUCKET"]

# response = SECRETS_MANAGER.get_secret_value(SecretId=DATABASE)
# CREDENTIALS = json.loads(response["SecretString"])


# def lambda_handler(event, _):

#     post_data = json.loads(event.get("body", "{}"))

#     authorized = check_auth(post_data["researcherID"])

#     if not authorized:
#         return format_response(403, "Not Authorized")

#     try:

#         database_url = URL.create(
#             drivername="postgresql+psycopg2",
#             host=CREDENTIALS["host"],
#             database=DATABASE,
#             username=CREDENTIALS["username"],
#             port=CREDENTIALS["port"],
#             password=CREDENTIALS["password"],
#             query={"sslmode": "verify-full", "sslrootcert": "./AmazonRootCA1.pem"},
#         )

#         engine = sqlalchemy.create_engine(database_url)

#         connection = engine.connect()

#         date = datetime.datetime.utcnow().date()

#         file_path = f"data_{datetime.datetime.strftime(date, '%Y_%m_%d')}"

#         s3_uri = f"aws_commons.create_s3_uri('{BUCKET}', '{file_path}', '{REGION}')"

#         connection.execute(
#             f"SELECT * from aws_s3.query_export_to_s3('SELECT * FROM records', {s3_uri}, options :='format csv');"
#         )

#         connection.close()

#         return format_response(200, "Data downloaded")

#     except Exception as e:
#         return format_response(403, e)
