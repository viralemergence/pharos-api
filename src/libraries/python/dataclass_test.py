from typing import Dict, Optional, Union
from enum import Enum

from pydantic import BaseModel, validator

from devtools import debug

data = """
{
"data": {
    "rec9Hjw3utQm2": {
        "Host species NCBI tax ID": {
            "displayValue": "3053800000",
            "dataValue": "30538000000",
            "report": {
                "status": "WARNING",
                "message": "Datapoint is not recognized."
            },
            "modifiedBy": "dev",
            "version": "1678391083301"
        },
        "Sample ID": {
            "displayValue": "1",
            "dataValue": "1",
            "report": {
                "status": "SUCCESS",
                "message": "Ready to release."
            },
            "modifiedBy": "dev",
            "version": "1678461815020"
        },
        "Host species": {
            "displayValue": "Bat",
            "dataValue": "Bat",
            "report": {
                "status": "SUCCESS",
                "message": "Ready to release."
            },
            "modifiedBy": "dev",
            "version": "1678461820035"
        },
        "Animal ID": {
            "displayValue": "Fred",
            "dataValue": "Fred",
            "report": {
                "status": "SUCCESS",
                "message": "Ready to release."
            },
            "modifiedBy": "dev",
            "version": "1678461824903",
            "previous": {
                "displayValue": "Old Name",
                "dataValue": "Old name",
                "modifiedBy": "dev",
                "version": "1678461824903"
            }
        }
    }
}
}
"""


class ReportScore(Enum):
    fail = "FAIL"
    success = "SUCCESS"
    warning = "WARNING"


class Report(BaseModel):
    status: ReportScore
    message: str
    data: Optional[dict] = None


class Datapoint(BaseModel):
    displayValue: str
    dataValue: Union[str, dict]
    modifiedBy: str
    version: str
    report: Optional[Report] = None
    previous: Optional["Datapoint"] = None


def SnakeCaseToSpaces(string: str) -> str:
    return string.replace("_", " ")


class Record(BaseModel):
    Host_species_NCBI_tax_ID: Optional[Datapoint] = None

    @validator("Host_species_NCBI_tax_ID")
    def length_check(cls, datapoint):
        if datapoint.report:
            return datapoint

        if len(datapoint.dataValue) > 8:
            datapoint.report.status = ReportScore.fail
            datapoint.report.message = "Datapoint is too long."

        return datapoint

    Sample_ID: Optional[Datapoint] = None
    Host_species: Optional[Datapoint] = None
    Animal_ID: Optional[Datapoint] = None

    class Config:
        alias_generator = SnakeCaseToSpaces


class Register(BaseModel):
    data: Dict[str, Record]


register = Register.parse_raw(data)

record = register.data["rec9Hjw3utQm2"]
animal_id = record.Animal_ID

status = animal_id.report.status.value
print(status)

animal_id.report.status = ReportScore.fail

status = animal_id.report.status.value
print(status)

prev = animal_id.previous
print(prev.dataValue)

ncbi = record.Host_species_NCBI_tax_ID

debug(ncbi)
