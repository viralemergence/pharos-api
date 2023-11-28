import os
from datetime import datetime
from typing import Union

import boto3
from auth import check_auth
from boto3.dynamodb.conditions import Key
from botocore.client import ClientError
from engine import get_engine
from format import format_response
from models import PublishedProject
from pydantic import BaseModel, Extra, Field, ValidationError
from register import Dataset, DatasetReleaseStatus, Project, ProjectPublishStatus
from sqlalchemy.orm import Session

DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]

CF_CLIENT = boto3.client("cloudfront")
CF_CACHE_POLICY_ID = os.environ["CF_CACHE_POLICY_ID"]


class UnpublishProjectData(BaseModel):
    project_id: str = Field(alias="projectID")

    class Config:
        extra = Extra.forbid


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
        validated = UnpublishProjectData.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    if not validated.project_id in user.project_ids:
        return format_response(403, "Researcher does not have access to this project")

    try:
        # Delete records from the database
        engine = get_engine()

        with Session(engine) as session:

            session.query(PublishedProject).where(
                PublishedProject.project_id == validated.project_id
            ).delete()

            session.commit()

    ## passing over this exception to go on to "reset"
    ## the metadata objects as well even if something is
    ## wrong with the database records
    except Exception as e:  # pylint: disable=broad-except
        print(e)

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
    published_datasets: list[Dataset] = []
    for item in metadata_response["Items"]:
        if item["sk"] == "_meta":
            project = Project.parse_table_item(item)
            continue

        dataset = Dataset.parse_table_item(item)
        if dataset.release_status in [
            DatasetReleaseStatus.PUBLISHED,
            # letting this reset datasets stuck in "publishing" state for now
            DatasetReleaseStatus.PUBLISHING,
        ]:
            published_datasets.append(dataset)

    if not project:
        return format_response(403, "Project not found")

    try:
        with METADATA_TABLE.batch_writer() as batch:
            # Update project metadata
            project.last_updated = datetime.utcnow().isoformat() + "Z"
            project.publish_status = ProjectPublishStatus.UNPUBLISHED
            batch.put_item(Item=project.table_item())

            # Update dataset metadata
            for dataset in published_datasets:
                # published datasets should go to released status
                dataset.last_updated = datetime.utcnow().isoformat() + "Z"
                dataset.release_status = DatasetReleaseStatus.RELEASED
                batch.put_item(Item=dataset.table_item())

    except ClientError as e:
        print(e)
        return format_response(403, "Error saving project and dataset metadata")

    distributions = CF_CLIENT.list_distributions_by_cache_policy_id(
        CachePolicyId=CF_CACHE_POLICY_ID
    )

    cf_id = distributions.get("DistributionIdList", {}).get("Items")[0]

    if cf_id:
        invalidation = CF_CLIENT.create_invalidation(
            DistributionId=cf_id,
            InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": ["/*"]},
                "CallerReference": f"{project.project_id}_{project.last_updated}",
            },
        )

        print(invalidation)

    return format_response(200, "Project records unpublished")
