import json
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Extra, Field, ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from auth import check_auth
from engine import get_engine
from format import format_response
from models import PublishedProject
from register import Project, ProjectPublishStatus


DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])


class SaveProjectBody(BaseModel):
    """Event data payload to save a project."""

    researcher_id: str = Field(..., alias="researcherID")
    project: Project

    class Config:
        extra = Extra.forbid


def update_published_project(project: Project):
    engine = get_engine()

    if not project.project_id:
        raise ValueError("Project must have a project_id")

    with Session(engine) as session:
        published_project = session.scalar(
            select(PublishedProject).where(
                PublishedProject.project_id == project.project_id
            )
        )

        if not published_project:
            raise ValueError("Project not found in database")

        published_project.name = project.name
        published_project.published_date = datetime.utcnow().date()
        published_project.description = project.description
        published_project.project_type = project.project_type
        published_project.surveillance_status = project.surveillance_status
        published_project.citation = project.citation
        published_project.related_materials = json.dumps(project.related_materials)
        published_project.project_publications = json.dumps(
            project.project_publications
        )
        published_project.others_citing = json.dumps(project.others_citing)

        session.commit()


def lambda_handler(event, _):
    try:
        validated = SaveProjectBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    user = check_auth(validated.researcher_id)

    if not user:
        return format_response(403, "Not Authorized")

    try:
        project_response = METADATA_TABLE.get_item(
            Key={"pk": validated.project.project_id, "sk": "_meta"}
        )

        if project_response.get("Item"):
            prev_project = Project.parse_table_item(project_response["Item"])

            # Check to make sure this researcher is an author on the project
            if prev_project.authors and not user.researcher_id in [
                author.researcher_id for author in prev_project.authors
            ]:
                return format_response(403, "Not Authorized")

            # merge the two projects, preserving the union of dataset_ids
            # regardless of which project is newer
            next_project = prev_project
            next_project.dataset_ids = prev_project.dataset_ids + list(
                set(validated.project.dataset_ids) - set(prev_project.dataset_ids)
            )

            if prev_project.last_updated and validated.project.last_updated:
                prev_updated = datetime.strptime(
                    prev_project.last_updated, "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                next_updated = datetime.strptime(
                    validated.project.last_updated, "%Y-%m-%dT%H:%M:%S.%fZ"
                )

                if next_updated > prev_updated:
                    # If the project was already published, update everything
                    # in the published project as well.
                    if prev_project.publish_status == ProjectPublishStatus.PUBLISHED:
                        update_published_project(validated.project)

            try:
                METADATA_TABLE.put_item(Item=next_project.table_item())
                return format_response(200, "Succesful upload")

            except ClientError as e:
                return format_response(500, e)

    except ClientError as e:
        # If the project does not already exist,
        # we can skip ahead to creating it
        pass

    try:
        METADATA_TABLE.put_item(Item=validated.project.table_item())
        return format_response(200, "Succesful upload")

    except ClientError as e:
        return format_response(500, e)
