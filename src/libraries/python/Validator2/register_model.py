import re
from typing import Dict, Optional, Union
from functools import wraps
from enum import Enum
from devtools import debug

from pydantic import BaseModel, Extra, validator


## Helper function to transform column names containing
## spaces to underscored names for use in the record class.
def snakeCaseToSpaces(string: str):
    return string.replace("_", " ")


## decorator to make the validator skip datapoints which
## already have a failure or warning, because they're already invalid
def validator_skip_fail_warn(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if args[1].report.status in (ReportScore.fail, ReportScore.warning):
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

    class Config:
        extra = Extra.forbid


class Report(BaseModel):
    """The report object a validated datapoint,
    containing both the score and a message to
    show the user in the frontend
    """

    status: ReportScore
    message: str
    data: Optional[dict] = None

    class Config:
        extra = Extra.forbid


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

    def float(self):
        """Return the datapoint's dataValue as an float, or if
        dataValue cannot be converted to an float, return None
        and add a warning report to the datapoint.
        """
        try:
            return float(self.dataValue)
        except ValueError:
            self.report = Report(
                status=ReportScore.fail,
                message="Value must be a decimal number",
            )
            return None

    def str(self):
        return str(self.dataValue)

    def int(self):
        """Return the datapoint's dataValue as an int, or if
        dataValue cannot be converted to an int, return None
        and add a warning report to the datapoint.
        """
        try:
            return int(self.dataValue)
        except ValueError:
            self.report = Report(
                status=ReportScore.fail, message="Value must be an integer"
            )
            return None

    def isnumeric(self):
        """Check if dataValue is numeric, and
        if not, add a warning to the datapoint.
        """
        if not self.dataValue.isnumeric():
            self.report = Report(
                status=ReportScore.fail, message="Value must be all numbers"
            )
            return False
        return True

    def __init__(self, **data) -> None:
        super().__init__(**data)
        if not self.report:
            self.report = Report(
                status=ReportScore.success, message="Ready to release."
            )

    class Config:
        extra = Extra.forbid


class Record(BaseModel):
    """The record object is displayed as a "row"
    to the user in the user interface, and closely
    approximates a row in the Pharos database.

    This model is used to both parse and validate
    the data; validation errors are added back to
    individual datapoints as a Report object for
    display in the user interface.
    """

    Sample_ID: Optional[Datapoint] = None
    Animal_ID: Optional[Datapoint] = None
    Host_species: Optional[Datapoint] = None
    Host_species_NCBI_tax_ID: Optional[Datapoint] = None
    Latitude: Optional[Datapoint] = None
    Longitude: Optional[Datapoint] = None
    Spatial_uncertainty: Optional[Datapoint] = None
    Collection_day: Optional[Datapoint] = None
    Collection_month: Optional[Datapoint] = None
    Collection_year: Optional[Datapoint] = None
    Collection_method_or_tissue: Optional[Datapoint] = None
    Detection_method: Optional[Datapoint] = None
    Primer_sequence: Optional[Datapoint] = None
    Primer_citation: Optional[Datapoint] = None
    Detection_target: Optional[Datapoint] = None
    Detection_target_NCBI_tax_ID: Optional[Datapoint] = None
    Detection_outcome: Optional[Datapoint] = None
    Detection_measurement: Optional[Datapoint] = None
    Detection_measurement_units: Optional[Datapoint] = None
    Pathogen: Optional[Datapoint] = None
    Pathogen_NCBI_tax_ID: Optional[Datapoint] = None
    GenBank_accession: Optional[Datapoint] = None
    Detection_comments: Optional[Datapoint] = None
    Organism_sex: Optional[Datapoint] = None
    Dead_or_alive: Optional[Datapoint] = None
    Health_notes: Optional[Datapoint] = None
    Life_stage: Optional[Datapoint] = None
    Age: Optional[Datapoint] = None
    Mass: Optional[Datapoint] = None
    Length: Optional[Datapoint] = None

    @validator(
        "Host_species_NCBI_tax_ID",
        "Detection_target_NCBI_tax_ID",
        "Pathogen_NCBI_tax_ID",
    )
    @validator_skip_fail_warn
    def length_check(cls, datapoint: Datapoint):
        if datapoint.isnumeric() and not 0 < len(datapoint.dataValue) < 8:
            datapoint.report = Report(
                status=ReportScore.fail,
                message="A NCBI taxonomic identifier consists of one to seven digits.",
            )
        return datapoint

    @validator("Latitude")
    @validator_skip_fail_warn
    def check_lat(cls, latitude: Datapoint):
        float_value = latitude.float()
        if float_value and not -90 <= float_value <= 90:
            latitude.report = Report(
                status=ReportScore.fail, message="Latitude must be between -90 and 90."
            )
        return latitude

    @validator("Longitude")
    @validator_skip_fail_warn
    def check_lon(cls, longitude: Datapoint):
        float_value = longitude.float()
        if float_value and not -180 <= float_value <= 180:
            longitude.report = Report(
                status=ReportScore.fail,
                message="Longitude must be between -180 and 180.",
            )
        return longitude

    class Config:
        ## datapoint names are transformed by
        ## replaceing spaces with underscores
        alias_generator = snakeCaseToSpaces

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


class Register(BaseModel):
    """The register object is the top-level
    object in the register system, containing
    the dictionary of all the records.
    """

    data: Dict[str, Record]
