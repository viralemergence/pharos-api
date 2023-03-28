"""
Register parsing and validation module.

A "dataset" in the Pharos UI is a register, which is a dictionary (or
object, in the JS frontend) of records. Each record has a set of optional
fields by default, which will be validated when the register is parsed,
but will also accept fields with any name. Each field is a datapoint,
and when the register is saved to the api, known fields are validated and
each datapoint gets a report which tells the system if it is ready
to import into the Pharos database, and if not, it includes a message
to help the user understand what they need to change. Unknown datapoints
are simply marked with a "WARNING" status.

Datapoints are a recursive data structure, where the `previous` property
points to the previous version of the datapoint. This allows us to track
the history of a datapoint, tracking when it changed, what changed, and
who made that change.

This is the core module which defines datapoints, validation reports,
and records.
"""

from datetime import datetime
from typing import Any, Dict, Optional
from functools import wraps
from enum import Enum

from pydantic import BaseModel, Extra, Field, validator

from column_alias import get_ui_name


REQUIRED_FIELDS = {
    "host_species",
    "latitude",
    "longitude",
    "collection_day",
    "collection_month",
    "collection_year",
    "detction_outcome",
    "pathogen",
}


class ReportScore(Enum):
    """The score of a validated datapoint, used for
    inclusion criteria into the final database.
    """

    FAIL = "FAIL"
    SUCCESS = "SUCCESS"
    WARNING = "WARNING"

    class Config:  # pylint: disable=too-few-public-methods
        extra = Extra.forbid


class Report(BaseModel):
    """The report object a validated datapoint,
    containing both the score and a message to
    show the user in the frontend.
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
    """Deprecating this; do not use."""

    dataValue: str
    """The value of the datapoint, as a string.
    In most cases this is the raw value the user entered,
    but in some cases it is a transformed from the user's
    selected unit into SI units.
    """

    modifiedBy: str
    """The researcherID of the user who modified this
    version of this datapoint.
    """

    version: int
    """The version of this datapoint, which is an integer timestamp
    used to reconcile divergent datapoint histories.
    """

    report: Optional[Report] = None
    """The report for this datapoint, once it has been validated."""

    previous: Optional["Datapoint"] = None
    """The previous version of this datapoint, if it exists."""

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
    report as long as the `Datapoint` is correctly formatted
    and structured. This initial report is often overridden
    by additional validation rules in the Record validator.
    """

    def __init__(self, **data) -> None:
        super().__init__(**data)
        if not self.report:
            self.report = Report(
                status=ReportScore.SUCCESS, message="Ready to release."
            )


## decorator to make the validator skip datapoints which
## already have a failure or warning, because they're already invalid
def validator_skip_fail_warn(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if args[1].report and args[1].report.status in (
            ReportScore.FAIL,
            ReportScore.WARNING,
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

    sample_id: Optional[DefaultPassDatapoint] = None
    animal_id: Optional[DefaultPassDatapoint] = None
    host_species: Optional[DefaultPassDatapoint] = None
    host_species_ncbi_tax_id: Optional[DefaultPassDatapoint] = None
    latitude: Optional[DefaultPassDatapoint] = None
    longitude: Optional[DefaultPassDatapoint] = None
    spatial_uncertainty: Optional[DefaultPassDatapoint] = None
    collection_day: Optional[Datapoint] = None
    collection_month: Optional[Datapoint] = None
    collection_year: Optional[Datapoint] = None
    collection_method_or_tissue: Optional[DefaultPassDatapoint] = None
    detection_method: Optional[DefaultPassDatapoint] = None
    primer_sequence: Optional[DefaultPassDatapoint] = None
    primer_citation: Optional[DefaultPassDatapoint] = None
    detection_target: Optional[DefaultPassDatapoint] = None
    detection_target_ncbi_tax_id: Optional[DefaultPassDatapoint] = None
    detection_outcome: Optional[DefaultPassDatapoint] = None
    detection_measurement: Optional[DefaultPassDatapoint] = None
    detection_measurement_units: Optional[DefaultPassDatapoint] = None
    pathogen: Optional[DefaultPassDatapoint] = None
    pathogen_ncbi_tax_id: Optional[DefaultPassDatapoint] = None
    genbank_accession: Optional[DefaultPassDatapoint] = None
    detection_comments: Optional[DefaultPassDatapoint] = None
    organism_sex: Optional[DefaultPassDatapoint] = None
    dead_or_alive: Optional[DefaultPassDatapoint] = None
    health_notes: Optional[DefaultPassDatapoint] = None
    life_stage: Optional[DefaultPassDatapoint] = None
    age: Optional[DefaultPassDatapoint] = None
    mass: Optional[DefaultPassDatapoint] = None
    length: Optional[DefaultPassDatapoint] = None

    @validator(
        "host_species_ncbi_tax_id",
        "detection_target_ncbi_tax_id",
        "pathogen_ncbi_tax_id",
    )
    @validator_skip_fail_warn
    def check_ncbi(cls, ncbi_id: DefaultPassDatapoint):
        """Check that the NCBI taxonomic identifier is
        numeric and between 1 and 7 digits long.
        """
        try:
            if int(ncbi_id) and not 0 < len(ncbi_id) < 8:
                ncbi_id.report = Report(
                    status=ReportScore.FAIL,
                    message="A NCBI taxonomic identifier consists of one to seven digits.",
                )
        except ValueError as e:
            ncbi_id.report = Report(status=ReportScore.FAIL, message=str(e))

        return ncbi_id

    @validator("latitude")
    @validator_skip_fail_warn
    def check_lat(cls, latitude: DefaultPassDatapoint):
        """Check that the latitude is numeric and between -90 and 90."""
        try:
            if not -90 <= float(latitude) <= 90:
                latitude.report = Report(
                    status=ReportScore.FAIL,
                    message="Latitude must be between -90 and 90.",
                )

        except ValueError as e:
            latitude.report = Report(status=ReportScore.FAIL, message=str(e))

        return latitude

    @validator("longitude")
    @validator_skip_fail_warn
    def check_lon(cls, longitude: DefaultPassDatapoint):
        """Check that the longitude is numeric and between -180 and 180."""
        try:
            if not -180 <= float(longitude) <= 180:
                longitude.report = Report(
                    status=ReportScore.FAIL,
                    message="Longitude must be between -180 and 180.",
                )

        except ValueError as e:
            longitude.report = Report(status=ReportScore.FAIL, message=str(e))

        return longitude

    @validator("collection_year")
    def check_date(cls, year: Datapoint, values: Dict[str, Datapoint]):
        """Check that the date is valid; skip validation if any of
        day, month, and year are missing, and check that the year is
        four digits long."""

        day = values.get("collection_day")
        month = values.get("collection_month")

        # Don't do any validation until all three are filled out
        if not day or not month:
            return year

        if len(year) < 4:
            year.report = Report(
                status=ReportScore.FAIL,
                message="Year must be a four-digit year",
            )
            return year

        try:
            date = datetime(int(year), int(month), int(day))
            report = Report(
                status=ReportScore.SUCCESS,
                message=f"Date {date.strftime('%Y-%m-%d')} is ready to release",
            )

        except ValueError as e:
            try:
                report = Report(
                    status=ReportScore.FAIL,
                    message=f"Date {int(year)}-{int(month)}-{int(day)} is invalid, {e}.",
                )
            except ValueError:
                report = Report(
                    status=ReportScore.FAIL,
                    message="All of day, month, and year must be numbers.",
                )

        day.report, month.report, year.report = report, report, report

        return year

    @validator("age", "mass", "length")
    @validator_skip_fail_warn
    def check_float(cls, value: DefaultPassDatapoint):
        """Check that the value is a decimal."""
        try:
            float(value)
        except ValueError:
            value.report = Report(
                status=ReportScore.FAIL,
                message=(
                    "Must be a number, units can be configured "
                    "in dataset settings (coming soon)."
                ),
            )

        return value

    class Config:
        ## datapoint names are transformed using
        ## a map between snake_case and UI names
        alias_generator = get_ui_name
        extra = Extra.allow

    def __init__(self, **data) -> None:
        super().__init__(**data)
        # Parse any unrecognized fields as Datapoints and add a warning report
        extra_fields = set(self.__dict__) - set(self.__fields__)
        for key in extra_fields:
            dat = Datapoint(**self.__dict__[key])
            dat.report = Report(
                status=ReportScore.WARNING, message="Datapoint is not recognized."
            )
            self.__dict__[key] = dat

    def __iter__(self):
        iterable: Dict[str, Datapoint] = self.__dict__
        return iter(iterable.items())


class ReleaseReport(BaseModel):
    released: bool = False
    successCount: int = 0
    warningCount: int = 0
    failCount: int = 0
    missingCount: int = 0
    warningFields: dict[str, list] = {}
    failFields: dict[str, list] = {}
    missingFields: dict[str, list] = {}


class Register(BaseModel):
    """The register object is the top-level
    object in the register system, containing
    the dictionary of all the records.
    """

    register_data: Dict[str, Record] = Field(..., alias="register")

    def get_release_report(self) -> ReleaseReport:
        """The release report summarizes all errors and
        warnings in the register, and adds additional
        information such as missing required fields which
        we want to test and display only when the user
        tries to release the dataset.
        """
        report = ReleaseReport()

        for recordID, record in self.register_data.items():
            for field in REQUIRED_FIELDS:
                if field not in record.__dict__:
                    report.missingCount += 1
                    if recordID not in report.missingFields:
                        report.missingFields[recordID] = []
                    report.missingFields[recordID].append(field)

            for field, datapoint in record:
                if datapoint.report is None:
                    # We can skip fields with no reports at this point
                    # because the only case where a field should not
                    # have a report after validation is when that report
                    # depends on another field, and that case will be
                    # caught by the missing fields check above.
                    continue

                if datapoint.report.status == ReportScore.SUCCESS:
                    report.successCount += 1
                    continue

                if datapoint.report.status == ReportScore.WARNING:
                    report.warningCount += 1
                    if recordID not in report.warningFields:
                        report.warningFields[recordID] = []
                    report.warningFields[recordID].append(field)
                    continue

                if datapoint.report.status == ReportScore.FAIL:
                    report.failCount += 1
                    if recordID not in report.failFields:
                        report.failFields[recordID] = []
                    report.failFields[recordID].append(field)

        if (
            report.missingCount == 0
            and report.failCount == 0
            and report.warningCount == 0
        ):
            report.released = True

        return report
