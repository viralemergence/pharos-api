import boto3
from pydantic import BaseModel, Extra, Field, ValidationError
from engine import get_engine
from format import format_response
from published_project import get_published_project_data


SECRETS_MANAGER = boto3.client("secretsmanager", region_name="us-west-1")


class QueryStringParameters(BaseModel):
    project_id: str = Field(alias="projectID")

    class Config:
        extra = Extra.forbid


class GetPublishedProjectEvent(BaseModel):
    query_string_parameters: QueryStringParameters = Field(
        alias="queryStringParameters"
    )

    class Config:
        extra = Extra.ignore


def lambda_handler(event, _):
    try:
        validated = GetPublishedProjectEvent.parse_obj(event)
    except ValidationError as e:
        return format_response(400, e.json(), preformatted=True)

    engine = get_engine()
    project_id = validated.query_string_parameters.project_id

    try:
        project_formatted = get_published_project_data(engine, project_id)
    except ValueError as e:
        return format_response(404, {"message": str(e)})

    return format_response(200, {"project": project_formatted})
