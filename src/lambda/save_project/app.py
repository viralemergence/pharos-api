import json
import os
import copy
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from pydantic import BaseModel, Extra, ValidationError
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


def merge_project(server_project: Project, remote_project: Project):
    # by default preserve the previous project
    next_project = copy.deepcopy(server_project)

    if server_project.last_updated and remote_project.last_updated:
        prev_updated = datetime.strptime(
            server_project.last_updated, "%Y-%m-%dT%H:%M:%S.%fZ"
        )
        next_updated = datetime.strptime(
            remote_project.last_updated, "%Y-%m-%dT%H:%M:%S.%fZ"
        )

        if next_updated > prev_updated:
            # If the project was already published, update everything
            # in the published project as well.
            if server_project.publish_status == ProjectPublishStatus.PUBLISHED:
                update_published_project(remote_project)

            # overwrite the old project with the new one
            next_project = remote_project

    # regardless of which project is newer, take the union of dataset_ids
    next_project.dataset_ids = server_project.dataset_ids + list(
        set(remote_project.dataset_ids) - set(server_project.dataset_ids)
    )

    # and the union of deleted_dataset_ids
    if not server_project.deleted_dataset_ids:
        server_project.deleted_dataset_ids = []
    if not remote_project.deleted_dataset_ids:
        remote_project.deleted_dataset_ids = []

    next_project.deleted_dataset_ids = server_project.deleted_dataset_ids + list(
        set(remote_project.deleted_dataset_ids)
        - set(server_project.deleted_dataset_ids)
    )

    # remove any dataset from dataset_ids that is in deleted_dataset_ids
    next_project.dataset_ids = list(
        set(next_project.dataset_ids) - set(next_project.deleted_dataset_ids)
    )

    return next_project


def lambda_handler(event, _):

    try:
        user = check_auth(event)
    except ValidationError:
        return format_response(403, "Not Authorized")

    if not user:
        return format_response(403, "Not Authorized")

    if not user.project_ids:
        return format_response(403, "Researcher has no projects")

    try:
        validated = SaveProjectBody.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    if not validated.project.project_id in user.project_ids:
        return format_response(403, "Researcher does not have access to this project")

    try:
        project_response = METADATA_TABLE.get_item(
            Key={"pk": validated.project.project_id, "sk": "_meta"}
        )

        if project_response.get("Item"):
            prev_project = Project.parse_table_item(project_response["Item"])
            next_project = merge_project(prev_project, validated.project)

            try:

                METADATA_TABLE.put_item(Item=next_project.table_item())
                return format_response(200, "Succesful upload")

            except ClientError as e:
                return format_response(500, e)
        else:
            # if the table response doesn't have an item, just save the incoming project
            try:
                METADATA_TABLE.put_item(Item=validated.project.table_item())
                return format_response(200, "Succesful upload")

            except ClientError as e:
                return format_response(500, e)

    except ClientError:
        # if the project doesn't exist, save it
        try:
            METADATA_TABLE.put_item(Item=validated.project.table_item())
            return format_response(200, "Succesful upload")

        except ClientError as e:
            return format_response(500, e)
