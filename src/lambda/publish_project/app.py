from datetime import datetime
import os
from typing import Union

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from pydantic import BaseModel, Extra, Field, ValidationError

from auth import check_auth
from format import format_response
from register import Dataset, DatasetReleaseStatus, Project, ProjectPublishStatus, User


DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]

LAMBDACLIENT = boto3.client("lambda")

PUBLISH_REGISTERS_LAMBDA = os.environ["PUBLISH_REGISTERS_LAMBDA"]


class PublishProjectData(BaseModel):
    """Event data payload to publish a project."""

    project_id: str = Field(..., alias="projectID")
    researcher_id: str = Field(..., alias="researcherID")

    class Config:
        extra = Extra.forbid


class PublishRegistersData(BaseModel):
    """Event data payload to publish the registers of the project."""

    project: Project
    released_datasets: list[Dataset]
    project_users: list[User]

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):  # pylint: disable=too-many-branches

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
        with METADATA_TABLE.batch_writer() as batch:
            # Update project metadata
            project.last_updated = datetime.utcnow().isoformat() + "Z"
            project.publish_status = ProjectPublishStatus.PUBLISHING
            batch.put_item(Item=project.table_item())

            # Update dataset metadata
            for dataset in released_datasets:
                dataset.last_updated = datetime.utcnow().isoformat() + "Z"
                dataset.release_status = DatasetReleaseStatus.PUBLISHING
                batch.put_item(Item=dataset.table_item())

    except ClientError as e:
        print(e)
        return format_response(403, "Error saving project and dataset metadata")

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

    ##
    ## Should return success here to the frontend, because
    ## it means the requirements to start publishing have
    ## been met; this is when the frontend should transition
    ## from "user clicked publish" status to "publish is
    ## actually happening on the server" status and start
    ## polling the dataset and project status.
    ##

    ##
    ## Need to move this section into separate, long-running lambda
    ## Frontend will update projects and datasets and status of each
    ## one will be managed in dynamodb
    ##

    LAMBDACLIENT.invoke(
        FunctionName=PUBLISH_REGISTERS_LAMBDA,
        InvocationType="Event",
        Payload=PublishRegistersData(
            project=project,
            released_datasets=released_datasets,
            project_users=project_users,
        ).json(by_alias=True),
    )

    return format_response(200, "Publishing Started")
