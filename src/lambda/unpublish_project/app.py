import os
import boto3
from pydantic import BaseModel, Extra, Field, ValidationError
from sqlalchemy.orm import Session

from auth import check_auth
from engine import get_engine
from format import format_response
from models2 import PublishedRecord


DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATASETS_S3_BUCKET = os.environ["DATASETS_S3_BUCKET"]


class UnpublishProjectData(BaseModel):
    researcher_id: str = Field(..., alias="researcherID")
    project_id: str = Field(..., alias="projectID")

    class Config:
        extra = Extra.forbid


def lambda_handler(event, _):
    try:
        validated = UnpublishProjectData.parse_raw(event.get("body", "{}"))
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    user = check_auth(validated.researcher_id)
    if not user or not user.project_ids or not validated.project_id in user.project_ids:
        return format_response(403, "Not Authorized")

    engine = get_engine()

    with Session(engine) as session:

        session.query(PublishedRecord).where(
            PublishedRecord.project_id == validated.project_id
        ).delete()

        session.commit()

    return format_response(200, "Project records unpublished")
