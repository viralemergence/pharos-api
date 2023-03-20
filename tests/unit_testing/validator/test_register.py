"""Tests for the register parsing and validation classes"""

import datetime
import pytest
from register import Record, ReportScore

VALID_RECORD = """
{
    "Host species": {
        "dataValue": "Vulpes vulpes",
        "modifiedBy": "dev",
        "version": "2",
        "previous": {
            "dataValue": "Previous Data Value",
            "modifiedBy": "dev",
            "version": "1"
        }
    },
    "Age": {
        "dataValue": "10",
        "modifiedBy": "dev",
        "version": "2",
        "previous": {
            "dataValue": "0",
            "modifiedBy": "dev",
            "version": "1"
        }
    },
    "Length": {
        "dataValue": "apple",
        "modifiedBy": "dev",
        "version": "2"
    }
}
"""


def test_valid_record():
    """Testing a basic, valid record"""
    record = Record.parse_raw(VALID_RECORD)
    assert record.Host_species is not None
    assert record.Host_species.dataValue == "Vulpes vulpes"
    assert record.Host_species.report is not None
    assert record.Host_species.report.status == ReportScore.SUCCESS
    assert record.Host_species.previous is not None
    assert record.Host_species.previous.dataValue == "Previous Data Value"
    assert record.Host_species.version == 2
    assert record.Host_species.previous.version == 1


def test_valid_coersion():
    """Testing coersions of data points which are expected to be successful"""
    record = Record.parse_raw(VALID_RECORD)
    assert record.Age is not None
    assert record.Age.dataValue == "10"
    assert float(record.Age) == 10.0
    assert str(record.Age) == "10"
    assert int(record.Age) == 10
    assert record.Age.nonzero_int() == 10
    assert record.Age.isnumeric() is True


def test_coersion_exception():
    """Testing coersions of data points which are expected to fail"""
    record = Record.parse_raw(VALID_RECORD)
    assert record.Host_species is not None
    assert record.Age is not None
    assert record.Age.previous is not None
    assert record.Length is not None
    assert record.Length.report is not None
    assert record.Length.report.status == ReportScore.FAIL

    with pytest.raises(ValueError):
        int(record.Host_species)

    with pytest.raises(ValueError):
        float(record.Host_species)

    with pytest.raises(ValueError):
        record.Host_species.nonzero_int()

    with pytest.raises(ValueError):
        record.Age.previous.nonzero_int()

    assert record.Host_species.isnumeric() is False


UNKNOWN_DATAPOINT = """
{
    "Unknown datapoint": {
        "dataValue": "something odd",
        "modifiedBy": "dev",
        "version": "1"
    }
}
"""


def test_unknown_datapoint():
    """Testing unknown datapoint warning"""
    record = Record.parse_raw(UNKNOWN_DATAPOINT)
    datapoint = getattr(record, "Unknown datapoint")
    assert datapoint is not None
    assert datapoint.report is not None
    assert datapoint.report.status == ReportScore.WARNING


DATAPOINT_ILLEGAL_ADDITIONAL_KEYS = """
{
    "Host species": {
        "dataValue": "Vulpes vulpes",
        "modifiedBy": "dev",
        "version": "2",
        "someOtherKey": "someOtherValue"
    }
}
"""


def test_datapoint_illegal_additional_keys():
    """Datapoints with additional keys should throw an exception"""
    with pytest.raises(ValueError):
        Record.parse_raw(DATAPOINT_ILLEGAL_ADDITIONAL_KEYS)


MISSING_REQUIRED_VALUE = """
{
    "Host species": {
        "dataValue": "Vulpes vulpes",
        "version": "2"
    }
}
"""


def test_missing_required_value():
    """Datapoints with missing required values should throw an exception"""
    with pytest.raises(ValueError):
        Record.parse_raw(MISSING_REQUIRED_VALUE)


REGISTER_WITH_PARTIAL_DATE = """
{
    "Collection day": {
        "dataValue": "1",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Collection year": {
        "dataValue": "112093",
        "modifiedBy": "dev",
        "version": "0"
    }
}
"""


def test_register_with_partial_date():
    """If a date is missing a day or month or year,
    no reports should be generated
    """
    record = Record.parse_raw(REGISTER_WITH_PARTIAL_DATE)
    assert record.Collection_day is not None
    assert record.Collection_day.report is None
    assert record.Collection_year is not None
    assert record.Collection_year.report is None


REGISTER_WITH_DATE = """
{
    "Collection day": {
        "dataValue": "1",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Collection month": {
        "dataValue": "1",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Collection year": {
        "dataValue": "2022",
        "modifiedBy": "dev",
        "version": "0"
    }
}
"""


def test_register_with_date():
    """If a date is complete, all datapoints in
    the date should get a success report
    """
    record = Record.parse_raw(REGISTER_WITH_DATE)
    assert record.Collection_day is not None
    assert record.Collection_day.report is not None
    assert record.Collection_month is not None
    assert record.Collection_month.report is not None
    assert record.Collection_year is not None
    assert record.Collection_year.report is not None

    assert record.Collection_day.report.status == ReportScore.SUCCESS
    assert record.Collection_month.report.status == ReportScore.SUCCESS
    assert record.Collection_year.report.status == ReportScore.SUCCESS

    date = datetime.date(
        int(record.Collection_year),
        int(record.Collection_month),
        int(record.Collection_day),
    )

    assert date == datetime.date(2022, 1, 1)


REGISTER_WITH_INVALID_DATE = """
{
    "Collection day": {
        "dataValue": "apple",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Collection month": {
        "dataValue": "100",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Collection year": {
        "dataValue": "2022",
        "modifiedBy": "dev",
        "version": "0"
    }
}
"""


def test_register_with_invalid_date():
    """If a date is invalid, all date datapoints should
    get a fail report with an explanation why.
    """
    record = Record.parse_raw(REGISTER_WITH_INVALID_DATE)
    assert record.Collection_day is not None
    assert record.Collection_day.report is not None
    assert record.Collection_month is not None
    assert record.Collection_month.report is not None
    assert record.Collection_year is not None
    assert record.Collection_year.report is not None

    print(record.Collection_day.report.status)

    assert record.Collection_day.report.status == ReportScore.FAIL
    assert record.Collection_month.report.status == ReportScore.FAIL
    assert record.Collection_year.report.status == ReportScore.FAIL


REGISTER_WITH_SHORT_YEAR = """
{
    "Collection day": {
        "dataValue": "1",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Collection month": {
        "dataValue": "1",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Collection year": {
        "dataValue": "202",
        "modifiedBy": "dev",
        "version": "0"
    }
}
"""


def test_register_with_short_year():
    """All years must have four digits"""
    record = Record.parse_raw(REGISTER_WITH_SHORT_YEAR)
    assert record.Collection_year is not None
    assert record.Collection_year.report is not None
    assert record.Collection_year.report.status == ReportScore.FAIL


VALID_LOCATION = """
{
    "Latitude": {
        "dataValue": "40.0150",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Longitude": {
        "dataValue": "105.2705",
        "modifiedBy": "dev",
        "version": "0"
    }
}
"""


def test_valid_location():
    """Testing a valid lat/lon pair"""
    record = Record.parse_raw(VALID_LOCATION)
    assert record.Latitude is not None
    assert record.Latitude.report is not None
    assert record.Latitude.report.status == ReportScore.SUCCESS

    assert record.Longitude is not None
    assert record.Longitude.report is not None
    assert record.Longitude.report.status == ReportScore.SUCCESS


NON_NUMERIC_INVALID_LOCATION = """
{
    "Latitude": {
        "dataValue": "plum",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Longitude": {
        "dataValue": "pear",
        "modifiedBy": "dev",
        "version": "0"
    }
}
"""


def test_non_numeric_invalid_location():
    """This lat/lon pair is invalid because they are not numeric"""
    record = Record.parse_raw(NON_NUMERIC_INVALID_LOCATION)
    assert record.Latitude is not None
    assert record.Latitude.report is not None
    assert record.Latitude.report.status == ReportScore.FAIL

    assert record.Longitude is not None
    assert record.Longitude.report is not None
    assert record.Longitude.report.status == ReportScore.FAIL


NUMERIC_INVALID_LOCATION = """
{
    "Latitude": {
        "dataValue": "400.0150",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Longitude": {
        "dataValue": "-1105.2705",
        "modifiedBy": "dev",
        "version": "0"
    }
}
"""


def test_invalid_location():
    """This lat/lon pair is invalid because they are out of bounds"""
    record = Record.parse_raw(NUMERIC_INVALID_LOCATION)
    assert record.Latitude is not None
    assert record.Latitude.report is not None
    assert record.Latitude.report.status == ReportScore.FAIL

    assert record.Longitude is not None
    assert record.Longitude.report is not None
    assert record.Longitude.report.status == ReportScore.FAIL


NCBI_TAX_IDS = """
{
    "Host species NCBI tax ID": {
        "dataValue": "9606",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Detection target NCBI tax ID": {
        "dataValue": "96123109812306",
        "modifiedBy": "dev",
        "version": "0"
    },
    "Pathogen NCBI tax ID": {
        "dataValue": "pears",
        "modifiedBy": "dev",
        "version": "0"
    }
}
"""


def test_ncbi_tax_ids():
    """Testing NCBI tax IDs, first should pass, second is too long, third is not numeric"""
    record = Record.parse_raw(NCBI_TAX_IDS)
    assert record.Host_species_NCBI_tax_ID is not None
    assert record.Host_species_NCBI_tax_ID.report is not None
    assert record.Host_species_NCBI_tax_ID.report.status == ReportScore.SUCCESS

    assert record.Detection_target_NCBI_tax_ID is not None
    assert record.Detection_target_NCBI_tax_ID.report is not None
    assert record.Detection_target_NCBI_tax_ID.report.status == ReportScore.FAIL

    assert record.Pathogen_NCBI_tax_ID is not None
    assert record.Pathogen_NCBI_tax_ID.report is not None
    assert record.Pathogen_NCBI_tax_ID.report.status == ReportScore.FAIL


EXISTING_WARNING = """
{
    "Mass": {
        "dataValue": "Vulpes vulpes",
        "modifiedBy": "dev",
        "version": "0",
        "report": {
            "status": "WARNING",
            "message": "don't modify me"
        }
    }
}
"""


def test_existing_warning():
    """If a datapoint already has a warning, it should not get tested again"""
    record = Record.parse_raw(EXISTING_WARNING)
    assert record.Mass is not None
    assert record.Mass.report is not None
    assert record.Mass.report.status == ReportScore.WARNING
    assert record.Mass.report.message == "don't modify me"
