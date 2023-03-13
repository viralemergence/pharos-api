from functools import wraps
from typing import Dict, Optional, Union
from enum import Enum

from pydantic import BaseModel, validator
from pydantic.config import Extra

from devtools import debug

example_data = """
{
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


## decorator to make the validator skip datapoints which
## already have a report, because they're already invalid
def validator_skip_existing_report(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if args[1].report:
            return args[1]
        return func(*args, **kwargs)

    return wrapper


class Record(BaseModel):
    Host_species_NCBI_tax_ID: Optional[Datapoint] = None

    @validator("Host_species_NCBI_tax_ID")
    @validator_skip_existing_report
    def length_check(cls, datapoint):
        if len(datapoint.dataValue) > 8:
            datapoint.report = Report(
                status=ReportScore.fail, message="Datapoint is too long."
            )

        return datapoint

    Sample_ID: Optional[Datapoint] = None
    Host_species: Optional[Datapoint] = None
    Animal_ID: Optional[Datapoint] = None

    def __init__(self, **data) -> None:
        super().__init__(**data)

        # Parse any unrecognized fields as Datapoints and add a warning report
        extra_fields = set(self.__dict__) - set(self.__fields__)

        for key in extra_fields:
            dat = Datapoint(**self.__dict__[key])

            dat.report = Report(
                status=ReportScore.warning, message="Datapoint is not recognized."
            )

            self.__dict__[key] = dat

    # @property
    # def extra_fields(self) -> set[str]:
    #     return set(self.__dict__) - set(self.__fields__)

    class Config:
        alias_generator = SnakeCaseToSpaces
        extra = Extra.allow


class Register(BaseModel):
    data: Dict[str, Record]


register = Register.parse_raw(f'{{"data":{example_data}}}')

record = register.data["rec9Hjw3utQm2"]

debug(record)


# print(record.extra_fields)

# for extra in record.extra_fields:
#     dat = Datapoint(**record.__dict__[extra])
#     dat.report = Report(
#         status=ReportScore.warning, message="Datapoint is not recognized."
#     )
#     record.__dict__[extra] = dat

# animal_id = record.Animal_ID

# debug(animal_id)

# status = animal_id.report.status.value

# animal_id.report.status = ReportScore.fail

# animal_report = animal_id.report

# prev = animal_id.previous

ncbi = record.Host_species_NCBI_tax_ID

# debug(ncbi)

# debug(record)

debug(record.__fields__)
debug(record.__dict__.keys())

print(register.json(indent=2, by_alias=True))
