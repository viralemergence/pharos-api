import json
import os
from datetime import datetime
from typing import TypedDict

import nanoid
import boto3
from pydantic import BaseModel
from sqlalchemy import select, sql
from sqlalchemy.orm import Session, load_only

from engine import get_engine
from format import CORS_ALLOW
from models import PublishedProject, Researcher
from register import User

CORS_ALLOW = os.environ["CORS_ALLOW"]

REGION = os.environ["REGION"]
DATA_DOWNLOAD_BUCKET_NAME = os.environ["DATA_DOWNLOAD_BUCKET_NAME"]

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

SES_CLIENT = boto3.client("ses", region_name=REGION)

# restrict alphabet and length, and add prefix
def generate_download_id():
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    return f"dwn{nanoid.generate(alphabet, 11)}"


class CreateExportData(BaseModel):
    """event payload to export a csv of published records"""

    user: User


class DataDownloadProject(TypedDict):
    projectID: str
    name: str


class DataDownloadResearcher(TypedDict):
    researcherID: str
    name: str


class DataDownloadMetadata(TypedDict):
    pk: str
    sk: str
    downloadDate: str
    projects: list[DataDownloadProject]
    researchers: list[DataDownloadResearcher]
    s3_uri: str


def lambda_handler(event, _):

    date = datetime.utcnow().date()
    file_path = f"data_{date.strftime('%Y_%m_%d')}"
    s3_uri = f"aws_commons.create_s3_uri('{DATA_DOWNLOAD_BUCKET_NAME}', '{file_path}', '{REGION}')"

    props = CreateExportData.parse_obj(event)
    user = props.user

    if not user.download_ids:
        user.download_ids = set()

    download_id = generate_download_id()
    user.download_ids.add(download_id)

    METADATA_TABLE.put_item(Item=user.table_item())

    print("user to save in table")
    print(user)

    engine = get_engine()

    data_download_metadata: DataDownloadMetadata = {
        "pk": download_id,
        "sk": "_meta",
        "downloadDate": datetime.utcnow().isoformat() + "Z",
        "projects": [],
        "researchers": [],
        "s3_uri": s3_uri,
    }

    with Session(engine) as session:
        projects = session.scalars(
            select(PublishedProject).options(
                load_only(PublishedProject.project_id, PublishedProject.name)
            )
        )

        for project in projects:
            data_download_metadata["projects"].append(
                {"name": project.name, "projectID": project.project_id}
            )

        researchers = session.scalars(
            select(Researcher).options(
                load_only(Researcher.name, Researcher.researcher_id)
            )
        )

        for researcher in researchers:
            data_download_metadata["researchers"].append(
                {
                    "name": researcher.name,
                    "researcherID": researcher.researcher_id,
                }
            )

        print(json.dumps(data_download_metadata))

        print("now query will execute")

        session.execute(
            sql.text(
                "SELECT * from aws_s3.query_export_to_s3("
                + "'SELECT * FROM published_records', "
                + f"{s3_uri}, options :='format csv, header'"
                + ");"
            )
        )

        email_response = SES_CLIENT.send_email(
            Source="no-reply@viralemergence.org",
            Destination={
                "ToAddresses": [user.email],
            },
            Message={
                "Subject": {
                    "Data": "Pharos Data Export ready for Download",
                },
                "Body": {
                    "Text": {
                        "Data": f"""
Your data export is ready for download. Please visit the following link to download your data:

{CORS_ALLOW}/d/?ref={download_id}

By downloading data from PHAROS, you are agreeing to properly credit researchers for their contributions by including the unique identifier for your download in the main text, data availability statement, or acknowledgements of your paper. Researchers who conduct work that benefits disproportionately from a small number of specific datasets should also consider engaging data data owners with an invitation to collaborate.

By participating in the community working with PHAROS, you are agreeing to conduct yourself with professionalism; to treat others without discrimination or disrespect; and to engage thoughtfully with issues around open science and data equity.
                        """
                    },
                },
            },
        )

        print(email_response)

    return True
