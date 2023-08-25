from typing import TypedDict

from sqlalchemy import Engine, select
from sqlalchemy.orm import Session
from models import Researcher


class ResearcherProjectAuthor(TypedDict):
    """Public researcher project author information for linking other authors"""

    researcherID: str
    name: str


class ResearcherProject(TypedDict):
    """Public researcher project information for consumption by the UI"""

    projectID: str
    name: str
    datePublished: str
    authors: list[ResearcherProjectAuthor]


class ResearcherFormatted(TypedDict):
    """Public researcher information for consumption by the UI"""

    researcherID: str
    name: str
    email: str
    organization: str
    projects: list[ResearcherProject]


class ResearchersResponse(TypedDict):
    """Response for researchers endpoint. This will need to
    be expanded include pagination and filtering information.
    """

    data: list[ResearcherFormatted]


def get_formatted_researchers(
    engine: Engine,
    researcher_ids: list[str],
) -> ResearchersResponse:
    """Get researchers matching the array of researcher_ids, or
    return all researchers if researcher_ids is empty, and format
    the data for consumption by the UI
    """

    with Session(engine) as session:

        researchers = select(Researcher).order_by(Researcher.name)

        if researcher_ids:
            researchers = researchers.where(
                Researcher.researcher_id.in_(researcher_ids)
            )

        researchers_formatted: list[ResearcherFormatted] = [
            {
                "researcherID": researcher.researcher_id,
                "name": researcher.name,
                "email": researcher.email,
                "organization": researcher.organization,
                "projects": [
                    {
                        "projectID": project.project_id,
                        "name": project.name,
                        "datePublished": project.published_date.isoformat() + "Z",
                        "authors": [
                            {
                                "researcherID": author.researcher_id,
                                "name": author.name,
                            }
                            for author in project.researchers
                        ],
                    }
                    for project in researcher.projects
                ],
            }
            for researcher in session.scalars(researchers).all()
        ]

        return {"data": researchers_formatted}
