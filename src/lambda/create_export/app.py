import os
from datetime import datetime
from typing import Set

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
from sqlalchemy.orm import Session, load_only

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

        # s3_uri = f"aws_commons.create_s3_uri('{DATA_DOWNLOAD_BUCKET_NAME}', '{s3_key}', '{REGION}')"

        # session.execute(
        #     sql.text(
        #         "SELECT * from aws_s3.query_export_to_s3("
        #         + "'SELECT * FROM published_records', "
        #         + f"{s3_uri}, options :='format csv, header'"
        #         + ");"
        #     )
        # )

        # cursor = session.connection().connection.cursor()

        # params = select(PublishedRecord).where(compound_filter).params()
        # params_tup = ()
        # for param in params:
        #     params_tup = (*params_tup, param)

        # # print("params")
        # print(str(select(PublishedRecord).where(compound_filter)))

        # print("filter")
        # print(
        #     cursor.mogrify(
        #         str(
        #             select(PublishedRecord).where(compound_filter)
        #             # .compile(dialect=postgresql.dialect())
        #         ).replace(":name_1", "%S"),
        #         ("Ryan Zimmerman+1")
        #         # select(PublishedRecord).where(compound_filter).params(),
        #     )
        #     # .compile(compile_kwargs={"literal_binds": True})
        # )

        statement = select("*").select_from(
            func.aws_s3.query_export_to_s3(
                str(
                    select(PublishedRecord)
                    .where(compound_filter)
                    .compile(compile_kwargs={"literal_binds": True})
                ),
                func.aws_commons.create_s3_uri(
                    DATA_DOWNLOAD_BUCKET_NAME, s3_key, REGION
                ),
                text("options := 'format csv, header'"),
            ).alias()
        )

        # f"{s3_uri}, options :='format csv, header'"
        print("statement")
        print(statement)

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
                    "Data": "Pharos Data Export ready for Download",
                },
                "Body": {
                    "Text": {
                        "Data": f"""
Your data export is ready for download. Please visit the following link to download your data:

{CORS_ALLOW}/d/?dwn={download_id}

By downloading data from PHAROS, you are agreeing to properly credit researchers for their contributions by including the unique identifier for your download in the main text, data availability statement, or acknowledgements of your paper. Researchers who conduct work that benefits disproportionately from a small number of specific datasets should also consider engaging data data owners with an invitation to collaborate.
                        """
                    },
                },
            },
        )

        print(email_response)

    return True
