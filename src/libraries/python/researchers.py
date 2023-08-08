from typing import TypedDict

from sqlalchemy import Engine, select
from sqlalchemy.orm import Session
from models import Researcher


class ResearcherFormatted(TypedDict):
    """Public researcher information for consumption by the UI"""

    researcherID: str
    name: str
    email: str
    organization: str
    projects: list[str]


def get_researchers(
    engine: Engine,
    researcher_ids: list[str],
) -> list[ResearcherFormatted]:
    """Get researchers matching the array of researcher_ids, or
    return all researchers if researcher_ids is empty, and format
    the data for consumption by the UI
    """

    with Session(engine) as session:

        researchers = select(Researcher)

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
                "projects": [project.project_id for project in researcher.projects],
            }
            for researcher in session.scalars(researchers).all()
        ]

        return researchers_formatted
