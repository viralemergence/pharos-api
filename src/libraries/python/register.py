from datetime import datetime
from typing import Dict, Optional, Union
from functools import wraps
from enum import Enum
from devtools import debug

from pydantic import BaseModel, Extra, Field, validator


## Helper function to transform column names containing
## spaces to underscored names for use in the record class.
def snakeCaseToSpaces(string: str):
    return string.replace("_", " ")


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

    displayValue: Optional[str]
    dataValue: Union[str, dict]
    modifiedBy: str
    version: int
    report: Optional[Report] = None
    previous: Optional["Datapoint"] = None

    def float(self):
        """Return the datapoint's dataValue as an float, or if
        dataValue cannot be converted to an float, return None
        and add a warning report to the datapoint.
        """
        if not self.dataValue:
            return None
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

    def nonzero_int(self):
        """Return the datapoint's non-zero dataValue as an int, or if
        dataValue cannot be converted to an int or is equal to zero,
        return None and add a warning report to the datapoint.
        """
        if self.int() and self.int() != 0:
            return self.int()

        self.report = Report(
            status=ReportScore.fail, message="Value must be a non-zero integer"
        )
        return None

    def isnumeric(self):
        """Check if dataValue is numeric, and
        if not, add a warning to the datapoint.
        """
        if not self.dataValue.isnumeric():
            self.report = Report(
                status=ReportScore.fail,
                message=(
                    "Value must be all numbers. Units can be "
                    "configured in the dataset settings (coming soon)."
                ),
            )
            return False
        return True

    def __len__(self):
        return len(self.dataValue)

    class Config:
        extra = Extra.forbid


class DefaultPassDatapoint(Datapoint):
    def __init__(self, **data) -> None:
        super().__init__(**data)
        if not self.report:
            self.report = Report(
                status=ReportScore.success, message="Ready to release."
            )


class NumericDatapoint(Datapoint):
    def __init__(self, **data) -> None:
        super().__init__(**data)
        if self.isnumeric() and not self.report:
            self.report = Report(
                status=ReportScore.success, message="Ready to release."
            )


## decorator to make the validator skip datapoints which
## already have a failure or warning, because they're already invalid
def validator_skip_fail_warn(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if args[1].report and args[1].report.status in (
            ReportScore.fail,
            ReportScore.warning,
        ):
            return args[1]
        return func(*args, **kwargs)

    return wrapper


class Record(BaseModel):
    """The record object is displayed as a "row"
    to the user in the user interface, and closely
    approximates a row in the Pharos database.

    This model is used to both parse and validate
    the data; validation errors are added back to
    individual datapoints as a Report object for
    display in the user interface.
    """

    Sample_ID: Optional[DefaultPassDatapoint] = None
    Animal_ID: Optional[DefaultPassDatapoint] = None
    Host_species: Optional[DefaultPassDatapoint] = None
    Host_species_NCBI_tax_ID: Optional[DefaultPassDatapoint] = None
    Latitude: Optional[DefaultPassDatapoint] = None
    Longitude: Optional[DefaultPassDatapoint] = None
    Spatial_uncertainty: Optional[DefaultPassDatapoint] = None
    Collection_day: Optional[Datapoint] = None
    Collection_month: Optional[Datapoint] = None
    Collection_year: Optional[Datapoint] = None
    Collection_method_or_tissue: Optional[DefaultPassDatapoint] = None
    Detection_method: Optional[DefaultPassDatapoint] = None
    Primer_sequence: Optional[DefaultPassDatapoint] = None
    Primer_citation: Optional[DefaultPassDatapoint] = None
    Detection_target: Optional[DefaultPassDatapoint] = None
    Detection_target_NCBI_tax_ID: Optional[DefaultPassDatapoint] = None
    Detection_outcome: Optional[DefaultPassDatapoint] = None
    Detection_measurement: Optional[DefaultPassDatapoint] = None
    Detection_measurement_units: Optional[DefaultPassDatapoint] = None
    Pathogen: Optional[DefaultPassDatapoint] = None
    Pathogen_NCBI_tax_ID: Optional[DefaultPassDatapoint] = None
    GenBank_accession: Optional[DefaultPassDatapoint] = None
    Detection_comments: Optional[DefaultPassDatapoint] = None
    Organism_sex: Optional[DefaultPassDatapoint] = None
    Dead_or_alive: Optional[DefaultPassDatapoint] = None
    Health_notes: Optional[DefaultPassDatapoint] = None
    Life_stage: Optional[DefaultPassDatapoint] = None
    Age: Optional[NumericDatapoint] = None
    Mass: Optional[NumericDatapoint] = None
    Length: Optional[NumericDatapoint] = None

    @validator(
        "Host_species_NCBI_tax_ID",
        "Detection_target_NCBI_tax_ID",
        "Pathogen_NCBI_tax_ID",
    )
    @validator_skip_fail_warn
    def length_check(cls, datapoint: Datapoint):
        if datapoint.int() and 0 < len(datapoint) < 8:
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

    @validator("Collection_year")
    def check_date(cls, year: Datapoint, values: Dict[str, Datapoint]):
        day = values.get("Collection_day")
        month = values.get("Collection_month")

        if len(year) < 4:
            year.report = Report(
                status=ReportScore.fail,
                message="Year must be a four-digit year",
            )
            return year

        try:
            if year.nonzero_int() and month.nonzero_int() and day.nonzero_int():
                try:
                    date = datetime(year.int(), month.int(), day.int())
                    report = Report(
                        status=ReportScore.success,
                        message=f"Date {date.strftime('%Y-%m-%d')} is ready to release",
                    )

                except ValueError as e:
                    debug(e)
                    report = Report(
                        status=ReportScore.fail,
                        message=f"Date {year.int()}-{month.int()}-{day.int()} is invalid, {e}.",
                    )

                day.report, month.report, year.report = report, report, report

        except AttributeError:
            # if month or day are None or cannot convert to int,
            # we don't need to add or overwrite any reports
            pass

        return year

    class Config:
        ## datapoint names are transformed by
        ## replaceing spaces with underscores
        alias_generator = snakeCaseToSpaces
        extra = Extra.allow

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


# class Register(BaseModel):
#     """The register object is the top-level
#     object in the register system, containing
#     the dictionary of all the records.
#     """

#     data: Dict[str, Record] = Field(..., alias="register")


#     @classmethod
#     def parse(cls, data: str):
#         """Parse a register json object and return a validated Register"""
#         return cls.parse_raw(f'{{"data": {data}}}')
