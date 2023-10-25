import os
from datetime import datetime

import boto3
from sqlalchemy import sql
from sqlalchemy.orm import Session

from engine import get_engine

REGION = os.environ["REGION"]
DATA_DOWNLOAD_BUCKET_NAME = os.environ["DATA_DOWNLOAD_BUCKET_NAME"]

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


def lambda_handler(event, _):
    date = datetime.utcnow().date()
    file_path = f"data_{date.strftime('%Y_%m_%d')}"
    s3_uri = f"aws_commons.create_s3_uri('{DATA_DOWNLOAD_BUCKET_NAME}', '{file_path}', '{REGION}')"

    engine = get_engine()
    with Session(engine) as session:
        session.execute(
            sql.text(
                "SELECT * from aws_s3.query_export_to_s3("
                + "'SELECT * FROM published_records', "
                + f"{s3_uri}, options :='format csv, header'"
                + ");"
            )
        )

    return True
