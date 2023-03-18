from datetime import datetime
from typing import Any, Dict, Optional
from functools import wraps
from enum import Enum

from pydantic import BaseModel, Extra, validator


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
    data: Optional[dict[str, Any]] = None

    class Config:
        extra = Extra.forbid


class Datapoint(BaseModel):
    """The datapoint object, the main unit of
    data in the register system. It contains
    the data, metadata, and the recursive
    history of the datapoint.
    """

    displayValue: Optional[str]
    dataValue: str
    modifiedBy: str
    version: int
    report: Optional[Report] = None
    previous: Optional["Datapoint"] = None

    def __float__(self):
        """Return the datapoint's dataValue as an float, or if
        dataValue cannot be converted to an float, raise a
        ValueError exception.
        """
        try:
            return float(self.dataValue)
        except ValueError as e:
            raise ValueError("Value must be a number") from e

    def __str__(self):
        return str(self.dataValue)

    def __int__(self):
        """Return the datapoint's dataValue as an int, or if
        dataValue cannot be converted to an int raise a
        ValueError exception.
        """
        try:
            return int(self.dataValue)
        except ValueError as e:
            raise ValueError("Value must be an integer") from e

    def __len__(self):
        return len(self.dataValue)

    def nonzero_int(self):
        """Return the datapoint's non-zero dataValue as an
        int, and if dataValue cannot be converted to an int
        int or is equal to zero, raise a ValueError exception.
        """
        if int(self) == 0:
            raise ValueError("Value must be a non-zero integer")

        return int(self)

    def isnumeric(self):
        """Check if dataValue is numeric."""
        return self.dataValue.isnumeric()

    class Config:
        extra = Extra.forbid


class DefaultPassDatapoint(Datapoint):
    """A Datapoint which automatically adds a passing
    report by default as long as it exists, so that it
    can be overridden by additional validation rules
    in the Record validation.
    """

    def __init__(self, **data) -> None:
        super().__init__(**data)
        if not self.report:
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
    Age: Optional[DefaultPassDatapoint] = None
    Mass: Optional[DefaultPassDatapoint] = None
    Length: Optional[DefaultPassDatapoint] = None

    @validator(
        "Host_species_NCBI_tax_ID",
        "Detection_target_NCBI_tax_ID",
        "Pathogen_NCBI_tax_ID",
    )
    @validator_skip_fail_warn
    def length_check(cls, ncbi_id: DefaultPassDatapoint):
        try:
            if int(ncbi_id) and not 0 < len(ncbi_id) < 8:
                ncbi_id.report = Report(
                    status=ReportScore.fail,
                    message="A NCBI taxonomic identifier consists of one to seven digits.",
                )
        except ValueError as e:
            ncbi_id.report = Report(status=ReportScore.fail, message=str(e))

        return ncbi_id

    @validator("Latitude")
    @validator_skip_fail_warn
    def check_lat(cls, latitude: DefaultPassDatapoint):
        try:
            if not -90 <= float(latitude) <= 90:
                latitude.report = Report(
                    status=ReportScore.fail,
                    message="Latitude must be between -90 and 90.",
                )

        except ValueError as e:
            latitude.report = Report(status=ReportScore.fail, message=str(e))

        return latitude

    @validator("Longitude")
    @validator_skip_fail_warn
    def check_lon(cls, longitude: DefaultPassDatapoint):
        try:
            if not -180 <= float(longitude) <= 180:
                longitude.report = Report(
                    status=ReportScore.fail,
                    message="Longitude must be between -180 and 180.",
                )

        except ValueError as e:
            longitude.report = Report(status=ReportScore.fail, message=str(e))

        return longitude

    @validator("Collection_year")
    def check_date(cls, year: Datapoint, values: Dict[str, Datapoint]):
        day = values.get("Collection_day")
        month = values.get("Collection_month")

        # Don't do any validation until all three are filled out
        if not day or not month:
            return year

        if len(year) < 4:
            year.report = Report(
                status=ReportScore.fail,
                message="Year must be a four-digit year",
            )
            return year

        try:
            date = datetime(int(year), int(month), int(day))
            report = Report(
                status=ReportScore.success,
                message=f"Date {date.strftime('%Y-%m-%d')} is ready to release",
            )

        except ValueError as e:
            try:
                report = Report(
                    status=ReportScore.fail,
                    message=f"Date {int(year)}-{int(month)}-{int(day)} is invalid, {e}.",
                )
            except ValueError:
                report = Report(
                    status=ReportScore.fail,
                    message="All of day, month, and year must be numbers.",
                )

        day.report, month.report, year.report = report, report, report

        return year

    @validator("Age", "Mass", "Length")
    @validator_skip_fail_warn
    def check_float(cls, value: DefaultPassDatapoint):
        try:
            float(value)
        except ValueError:
            value.report = Report(
                status=ReportScore.fail,
                message=(
                    "Must be a number, units can be configured "
                    "in dataset settings (coming soon)."
                ),
            )

        return value

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
