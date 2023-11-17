import os
from datetime import datetime

import boto3
from data_downloads import (
    CreateExportDataEvent,
    DataDownloadMetadata,
    DataDownloadProject,
    DataDownloadResearcher,
    generate_download_id,
)
from engine import get_engine
from format import CORS_ALLOW
from models import PublishedDataset, PublishedProject, PublishedRecord, Researcher
from published_records import get_compound_filter
from sqlalchemy import func, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

CORS_ALLOW = os.environ["CORS_ALLOW"]

REGION = os.environ["REGION"]
DATA_DOWNLOAD_BUCKET_NAME = os.environ["DATA_DOWNLOAD_BUCKET_NAME"]

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

SES_CLIENT = boto3.client("ses", region_name=REGION)


def lambda_handler(event, _):

    props = CreateExportDataEvent.parse_obj(event)
    user = props.user

    if not user.download_ids:
        user.download_ids = set()

    download_id = generate_download_id()
    user.download_ids.add(download_id)

    engine = get_engine()

    # In the future, we should check if any of the data
    # is actually different from prior exports and only
    # create a new s3 object if it is actually unique.

    # The explicit separation here between the ID and
    # the s3 key is to make it easier to add later.

    # Or, you know, implement enough versioning in the
    # database to do this correctly once publishing and
    # unpublishing workflow requirements are stable.
    s3_key = f"{download_id}/data.csv"

    data_download_metadata = DataDownloadMetadata(
        downloadID=download_id,
        downloadDate=datetime.utcnow().isoformat() + "Z",
        queryStringParameters=props.query_string_parameters,
        s3_key=s3_key,
    )

    with Session(engine) as session:
        (compound_filter, _) = get_compound_filter(props.query_string_parameters)

        projects = (
            session.query(
                PublishedProject.project_id,
                PublishedProject.name,
            )
            .join(PublishedDataset)
            .join(PublishedRecord)
            .where(compound_filter)
            .distinct(PublishedProject.project_id)
        )

        project_ids = set()

        for project in projects.all():
            project_ids.add(project.project_id)
            data_download_metadata.projects.append(
                DataDownloadProject(
                    projectID=project.project_id,
                    name=project.name,
                )
            )

        researchers = session.scalars(
            select(Researcher)
            .where(
                Researcher.projects.any(PublishedProject.project_id.in_(project_ids))
            )
            .distinct(Researcher.researcher_id)
        ).all()

        for researcher in researchers:
            data_download_metadata.researchers.append(
                DataDownloadResearcher(
                    researcherID=researcher.researcher_id,
                    name=researcher.name,
                )
            )

        cursor = session.connection().connection.cursor()

        columns_to_skip = ["geom"]
        columns_to_select = [
            col
            for col in PublishedRecord.__table__.columns
            if col.key not in columns_to_skip
        ]

        compiled_data_query = (
            select(
                *columns_to_select,
                PublishedRecord.geom.ST_Y().label("latitude"),
                PublishedRecord.geom.ST_X().label("longitude"),
                PublishedRecord.geom.ST_AsText().label("wkt"),
            )
            .where(compound_filter)
            .compile(
                dialect=postgresql.dialect(),
            )
        )

        escaped_data_query = (
            cursor.mogrify(str(compiled_data_query), compiled_data_query.params).decode(
                "utf-8"
            ),
        )

        statement = select("*").select_from(
            func.aws_s3.query_export_to_s3(
                escaped_data_query,
                func.aws_commons.create_s3_uri(
                    DATA_DOWNLOAD_BUCKET_NAME, s3_key, REGION
                ),
                text("options := 'format csv, header'"),
            ).alias()
        )

        session.execute(statement)

        METADATA_TABLE.put_item(Item=data_download_metadata.table_item())
        METADATA_TABLE.put_item(Item=user.table_item())

        email_response = SES_CLIENT.send_email(
            Source="no-reply@viralemergence.org",
            Destination={
                "ToAddresses": [user.email],
            },
            Message={
                "Subject": {
                    "Data": "Pharos data extract ready for Download",
                },
                "Body": {
                    "Text": {
                        "Data": f"""
Your data extract is ready for download. Please visit the following link to download your data:

{CORS_ALLOW}/d/?dwn={download_id}

By downloading data from PHAROS, you are agreeing to properly credit researchers for their contributions by including the unique identifier for your download in the main text, data availability statement, or acknowledgements of your paper. Researchers who conduct work that benefits disproportionately from a small number of specific datasets should also consider engaging data data owners with an invitation to collaborate.
                        """
                    },
                },
            },
        )

        print(email_response)

    return True
