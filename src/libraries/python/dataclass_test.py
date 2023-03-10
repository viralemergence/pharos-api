from typing import Dict, Optional
from enum import Enum

from pydantic import BaseModel, validator

from devtools import debug

data = """
{
"data": {
    "rec9Hjw3utQm2": {
        "Host_species_NCBI_tax_ID": {
            "displayValue": "3053800000",
            "dataValue": "30538000000",
            "report": {
                "status": "WARNING",
                "message": "Datapoint is not recognized."
            },
            "modifiedBy": "dev",
            "version": "1678391083301"
        },
        "Sample_ID": {
            "displayValue": "1",
            "dataValue": "1",
            "report": {
                "status": "SUCCESS",
                "message": "Ready to release."
            },
            "modifiedBy": "dev",
            "version": "1678461815020"
        },
        "Host_species": {
            "displayValue": "Bat",
            "dataValue": "Bat",
            "report": {
                "status": "SUCCESS",
                "message": "Ready to release."
            },
            "modifiedBy": "dev",
            "version": "1678461820035"
        },
        "Animal_ID": {
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
    dataValue: str
    modifiedBy: str
    version: str
    report: Optional[Report] = None
    previous: Optional["Datapoint"] = None


class Record(BaseModel):
    Host_species_NCBI_tax_ID: Optional[Datapoint] = None

    @validator("Host_species_NCBI_tax_ID")
    def length_check(cls, value):
        if len(value.dataValue) > 8:
            value.report.status = ReportScore.fail
            value.report.message = "Datapoint is too long."
        return value

    Sample_ID: Optional[Datapoint] = None
    Host_species: Optional[Datapoint] = None
    Animal_ID: Optional[Datapoint] = None


class Register(BaseModel):
    data: Dict[str, Record]


register = Register.parse_raw(data)

record = register.data["rec9Hjw3utQm2"]
datapoint = record.Animal_ID

status = datapoint.report.status.value
print(status)

datapoint.report.status = ReportScore.fail

status = datapoint.report.status.value
print(status)

prev = datapoint.previous
print(prev.dataValue)

debug(record.Host_species_NCBI_tax_ID)
