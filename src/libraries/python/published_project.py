import json
from typing import TypedDict

from sqlalchemy import Engine, select
from sqlalchemy.orm import Session

from models import PublishedProject


class DatasetFormatted(TypedDict):
    """Published dataset formatted for consumption by the UI."""
    datasetID: str
    name: str

class AuthorFormatted(TypedDict):
    """Published author formatted for consumption by the UI."""
    name: str
    organization: str
    researcherID: str

class ProjectFormatted(TypedDict):
    """Published project formatted for consumption by the UI."""
    projectID: str
    name: str
    datePublished: str
    description: str | None
    projectType: str | None
    surveillanceStatus: str | None
    citation: str | None
    relatedMaterials: list[str]
    projectPublications: list[str]
    othersCiting: list[str]
    authors: list[AuthorFormatted]
    datasets: list[DatasetFormatted]


def get_published_project_data(
    engine: Engine,
    project_id: str,
) -> ProjectFormatted:
    with Session(engine) as session:
        project = session.scalar(
            select(
                PublishedProject,
            ).where(PublishedProject.project_id == project_id)
        )

        if not project:
            raise ValueError(f'Project "{project_id}" not found')

        datasets_formatted: list[DatasetFormatted] = [
            {
                "datasetID": dataset.dataset_id,
                "name": dataset.name
            }
            for dataset in project.datasets
        ]

        authors_formatted: list[AuthorFormatted] = [
            {
                "name": researcher.name,
                "organization": researcher.organization,
                "researcherID": researcher.researcher_id,
            }
            for researcher in project.researchers
        ]

    project_formatted: ProjectFormatted = {
        "projectID": project_id,
        "name": project.name,
        "description": project.description,
        "datePublished": project.published_date.isoformat() + "Z",
        "projectType": project.project_type,
        "surveillanceStatus": project.surveillance_status,
        "citation": project.citation,
        "authors": authors_formatted,
        "datasets": datasets_formatted,
        "relatedMaterials": [],
        "projectPublications": [],
        "othersCiting": [],
    }

    if project.related_materials:
        project_formatted["relatedMaterials"] = json.loads(project.related_materials)

    if project.project_publications:
        project_formatted["projectPublications"] = json.loads(project.project_publications)

    if project.others_citing:
        project_formatted["othersCiting"] = json.loads(project.others_citing)


    return project_formatted
