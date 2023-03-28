# pylint: skip-file
# import os
# import json
# import boto3
# import sqlalchemy
# from sqlalchemy.engine import URL
# from sqlalchemy.orm import sessionmaker

# # from auth import check_auth
# from format import format_response
# from models import Tests, ResearchersTests
# from create_record import create_records, verify_record

# SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")

# DYNAMODB = boto3.resource("dynamodb")
# PROJECTS_TABLE = DYNAMODB.Table(os.environ["PROJECTS_TABLE_NAME"])
# DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])

# S3CLIENT = boto3.client("s3")
# DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]

# RDS = boto3.client("rds")
# DATABASE = os.environ["DATABASE"]

# response = SECRETS_MANAGER.get_secret_value(SecretId=DATABASE)
# CREDENTIALS = json.loads(response["SecretString"])


# def lambda_handler(event, _):

#     post_data = json.loads(event.get("body", "{}"))

#     # Retrieve datasets from project table
#     print("Retrieve datasets from project table")
#     try:

#         project = PROJECTS_TABLE.get_item(Key={"projectID": post_data["projectID"]})
#         datasets_ids = project["Item"]["datasetIDs"]

#     except Exception as e:
#         return format_response(403, e)

#     # Retrieve meta from datasets and filter released
#     print("Retrieve meta from datasets and filter released")
#     try:
#         for dataset_id in datasets_ids:
#             dataset_meta = DATASETS_TABLE.get_item(
#                 Key={"datasetID": dataset_id, "recordID": "_meta"}
#             )
#             if dataset_meta["Item"]["releaseStatus"] != "Released":
#                 datasets_ids.remove(dataset_id)

#     except Exception as e:
#         return format_response(403, e)

#     try:

#         print("Connect to database")
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

#         Session = sessionmaker(bind=engine)

#         Tests.__table__.create(engine, checkfirst=True)
#         ResearchersTests.__table__.create(engine, checkfirst=True)

#         with Session() as session:

#             print("Session established")
#             records = []

#             for dataset_id in datasets_ids:
#                 print(f"Ingesting dataset: {dataset_id}")
#                 # Retrieve last version of the register
#                 key_list = S3CLIENT.list_objects_v2(
#                     Bucket=DATASETS_S3_BUCKET, Prefix=f"{dataset_id}/"
#                 )["Contents"]

#                 key_list.sort(key=lambda item: item["LastModified"], reverse=True)
#                 key = key_list[0]["Key"]

#                 register_response = S3CLIENT.get_object(
#                     Bucket=DATASETS_S3_BUCKET, Key=key
#                 )
#                 register = register_response["Body"].read().decode("UTF-8")

#                 register = json.loads(register)

#                 # Create records
#                 for record_id, record in register.items():

#                     is_valid_record = verify_record(record)

#                     if not is_valid_record:
#                         return format_response(400, "Invalid record found.")

#                     test_record, researcher_test_record = create_records(
#                         post_data["projectID"],
#                         dataset_id,
#                         record_id,
#                         post_data["researcherID"],
#                         record,
#                     )

#                     records.extend([test_record, researcher_test_record])

#             # Upload records
#             print("Commit records to db")
#             session.add_all(records)
#             session.commit()

#         return format_response(200, "Works")

#     except Exception as e:
#         return format_response(403, e)
