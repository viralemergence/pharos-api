import os

import boto3
from pydantic import BaseModel, Extra, Field, ValidationError
from data_downloads import DataDownloadMetadata

from format import format_response


DYNAMODB = boto3.resource("dynamodb")
METADATA_TABLE = DYNAMODB.Table(os.environ["METADATA_TABLE_NAME"])

S3CLIENT = boto3.client("s3")
DATA_DOWNLOAD_BUCKET_NAME = os.environ["DATA_DOWNLOAD_BUCKET_NAME"]


class DownloadMetadataQueryStringParameters(BaseModel):
    """Query string parameters for a pharos export."""

    download_id: str = Field(alias="downloadID")

    class Config:
        extra = Extra.forbid


class DownloadMetadataEvent(BaseModel):
    """Event data payload to load metadata for a pharos export."""

    query_string_parameters: DownloadMetadataQueryStringParameters = Field(
        alias="queryStringParameters"
    )

    class Config:
        extra = Extra.ignore


def lambda_handler(event, _):

    try:
        validated = DownloadMetadataEvent.parse_obj(event)
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    table_response = METADATA_TABLE.get_item(
        Key={
            "pk": validated.query_string_parameters.download_id,
            "sk": "_meta",
        }
    )

    if "Item" not in table_response:
        return format_response(404, "Export not found")

    metadata = DataDownloadMetadata.parse_table_item(table_response["Item"])

    access_link = S3CLIENT.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": DATA_DOWNLOAD_BUCKET_NAME, "Key": metadata.s3_key},
    )

    response = metadata.format_response()
    response["accessLink"] = access_link

    return format_response(200, response)
