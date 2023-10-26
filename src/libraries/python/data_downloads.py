from pydantic import BaseModel

from register import User


class CreateExportData(BaseModel):
    """event payload to export a csv of published records"""

    user: User
