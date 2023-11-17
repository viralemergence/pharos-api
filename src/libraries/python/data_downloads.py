import nanoid
from published_records import FiltersQueryStringParameters
from pydantic import BaseModel, Extra, Field
from register import User


# restrict alphabet and length, and add prefix
def generate_download_id():
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    return f"dwn{nanoid.generate(alphabet, 11)}"


class CreateExportDataEvent(BaseModel):
    """event payload to export a csv of published records"""

    user: User
    query_string_parameters: FiltersQueryStringParameters = Field(
        alias="queryStringParameters"
    )


class DataDownloadProject(BaseModel):
    project_id: str = Field(alias="projectID")
    name: str

    class Config:
        extra = Extra.forbid


class DataDownloadResearcher(BaseModel):
    researcher_id: str = Field(alias="researcherID")
    name: str

    class Config:
        extra = Extra.forbid


class DataDownloadMetadata(BaseModel):
    download_id: str = Field(alias="downloadID")
    download_date: str = Field(alias="downloadDate")
    projects: list[DataDownloadProject] = Field(default_factory=list)
    researchers: list[DataDownloadResearcher] = Field(default_factory=list)
    query_string_parameters: FiltersQueryStringParameters = Field(
        alias="queryStringParameters"
    )
    s3_key: str

    class Config:
        extra = Extra.forbid

    def table_item(self):
        item = self.dict(by_alias=True)
        item["pk"] = self.download_id
        item["sk"] = "_meta"
        return item

    @classmethod
    def parse_table_item(cls, item):
        item["downloadID"] = item["pk"]
        del item["pk"]
        del item["sk"]
        return cls.parse_obj(item)

    def format_response(self):
        response = self.dict(by_alias=True)
        del response["s3_key"]
        return response
