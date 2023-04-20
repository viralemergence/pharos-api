import os
from typing import Union

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from pydantic import BaseModel, Extra, Field, ValidationError
from sqlalchemy.orm import Session

from auth import check_auth
from engine import get_engine
from format import format_response
from models import Researcher
from publish_register import publish_register_to_session
from register import Dataset, DatasetReleaseStatus, Project, Register, User

SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class PublishProjectData(BaseModel):
    """Event data payload to publish a project."""

    project_id: str = Field(..., alias="projectID")
    researcher_id: str = Field(..., alias="researcherID")

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):

    try:
        validated = PublishProjectData.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    user = check_auth(validated.researcher_id)
    if not user or not user.project_ids or not validated.project_id in user.project_ids:
        return format_response(403, "Not Authorized")

    try:
        # Retrieve project metadata and datasets
        metadata_response = METADATA_TABLE.query(
            KeyConditionExpression=Key("pk").eq(validated.project_id)
        )

    except ClientError as e:
        print(e)
        return format_response(403, "Error retrieving project metadata")

    if not metadata_response["Items"]:
        return format_response(403, "Metadata not found")

    project: Union[Project, None] = None
    released_datasets: list[Dataset] = []
    for item in metadata_response["Items"]:
        if item["sk"] == "_meta":
            project = Project.parse_table_item(item)
            continue

        dataset = Dataset.parse_table_item(item)
        if dataset.release_status == DatasetReleaseStatus.RELEASED:
            released_datasets.append(dataset)

    if not project:
        return format_response(403, "Project not found")

    if len(released_datasets) == 0:
        return format_response(403, "No released datasets found")

    if not project.authors:
        return format_response(403, "No authors found")

    try:
        # Retrieve project authors
        users_metadata = DYNAMODB.batch_get_item(
            RequestItems={
                METADATA_TABLE.name: {
                    "Keys": [
                        {"pk": author.researcher_id, "sk": "_meta"}
                        for author in project.authors
                    ]
                }
            }
        )
    except ClientError as e:
        print(e)
        return format_response(403, "Error retrieving researchers")

    project_users: list[User] = [
        User.parse_table_item(item)
        for item in users_metadata["Responses"][METADATA_TABLE.name]
    ]

    if len(project_users) == 0:
        return format_response(403, "No authors found")

    engine = get_engine()

    with Session(engine) as session:
        researcher_ids = [user.researcher_id for user in project_users]

        # Get existing researchers
        existing_researchers = (
            session.query(Researcher)
            .filter(Researcher.researcher_id.in_(researcher_ids))
            .all()
        )

        existing_researcher_ids = [r.researcher_id for r in existing_researchers]

        new_researchers: list[Researcher] = []

        # Add new researchers
        for user in project_users:
            if user.researcher_id in existing_researcher_ids:
                continue

            new_researchers.append(
                Researcher(researcher_id=user.researcher_id, name=user.name)
            )

        session.add_all(new_researchers)

        for dataset in released_datasets:
            key = f"{dataset.dataset_id}/data.json"
            register_json = (
                S3CLIENT.get_object(Bucket=DATASETS_S3_BUCKET, Key=key)["Body"]
                .read()
                .decode("utf-8")
            )

            register = Register.parse_raw(register_json)

            publish_register_to_session(
                session=session,
                register=register,
                project_id=project.project_id,
                dataset_id=dataset.dataset_id,
                researchers=existing_researchers + new_researchers,
            )

        session.commit()

    return format_response(200, "Success")
