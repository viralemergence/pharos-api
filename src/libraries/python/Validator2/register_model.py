from typing import Dict, Optional, Union
from functools import wraps
from enum import Enum

from pydantic import BaseModel, Extra, validator


## Helper function to transform column names containing
## spaces to underscored names for use in the record class.
def snakeCaseToSpaces(string: str):
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


class ReportScore(Enum):
    """The score of a validated datapoint, used for
    inclusion criteria into the final database.
    """

    fail = "FAIL"
    success = "SUCCESS"
    warning = "WARNING"


class Report(BaseModel):
    """The report object a validated datapoint,
    containing both the score and a message to
    show the user in the frontend
    """

    status: ReportScore
    message: str
    data: Optional[dict] = None


class Datapoint(BaseModel):
    """The datapoint object, the main unit of
    data in the register system. It contains
    the data, metadata, and the recursive
    history of the datapoint.
    """

    displayValue: str
    dataValue: Union[str, dict]
    modifiedBy: str
    version: str
    report: Optional[Report] = None
    previous: Optional["Datapoint"] = None


class Record(BaseModel):
    """The record object is displayed as a "row"
    to the user in the user interface, and closely
    approximates a row in the Pharos database.

    This model is used to both parse and validate
    the data; validation errors are added back to
    individual datapoints as a Report object for
    display in the user interface.
    """

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

    class Config:
        ## datapoint names are transformed by
        ## replaceing spaces with underscores
        alias_generator = snakeCaseToSpaces
        extra = Extra.forbid


class Register(BaseModel):
    """The register object is the top-level
    object in the register system, containing
    the dictionary of all the records.
    """

    data: Dict[str, Record]
