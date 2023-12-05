"""Tests for the register parsing and validation classes"""

import datetime

import pytest
from register import Datapoint, DatasetReleaseStatus, Record, Register, ReportScore

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
    assert record.host_species is not None
    assert record.host_species.data_value == "Vulpes vulpes"
    assert record.host_species.report is not None
    assert record.host_species.report.status == ReportScore.SUCCESS
    assert record.host_species.previous is not None
    assert record.host_species.previous.data_value == "Previous Data Value"
    assert record.host_species.version == 2
    assert record.host_species.previous.version == 1


def test_valid_coersion():
    """Testing coersions of data points which are expected to be successful"""
    record = Record.parse_raw(VALID_RECORD)
    assert record.age is not None
    assert record.age.data_value == "10"
    assert float(record.age) == 10.0
    assert str(record.age) == "10"
    assert int(record.age) == 10
    assert record.age.nonzero_int() == 10
    assert record.age.isnumeric() is True


def test_coersion_exception():
    """Testing coersions of data points which are expected to fail"""
    record = Record.parse_raw(VALID_RECORD)
    assert record.host_species is not None
    assert record.age is not None
    assert record.age.previous is not None
    assert record.length is not None
    assert record.length.report is not None
    assert record.length.report.status == ReportScore.FAIL

    with pytest.raises(ValueError):
        int(record.host_species)

    with pytest.raises(ValueError):
        float(record.host_species)

    with pytest.raises(ValueError):
        record.host_species.nonzero_int()

    with pytest.raises(ValueError):
        record.age.previous.nonzero_int()

    assert record.host_species.isnumeric() is False


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


DATAPOINT_ILLEGAL_KEYS = """
{
    "Host species": {
        "dataValue": "Vulpes vulpes",
        "modifiedBy": "dev",
        "version": "2",
        "someOtherKey": "someOtherValue"
    }
}
"""


def test_datapoint_illegal_keys():
    """Datapoints with additional keys should throw an exception"""
    with pytest.raises(ValueError):
        Record.parse_raw(DATAPOINT_ILLEGAL_KEYS)


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


def test_partial_date():
    """If a date is missing a day or month or year,
    no reports should be generated
    """
    record = Record.parse_raw(REGISTER_WITH_PARTIAL_DATE)
    assert record.collection_day is not None
    assert record.collection_day.report is None
    assert record.collection_year is not None
    assert record.collection_year.report is None


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


def test_date():
    """If a date is complete, all datapoints in
    the date should get a success report
    """
    record = Record.parse_raw(REGISTER_WITH_DATE)
    assert record.collection_day is not None
    assert record.collection_day.report is not None
    assert record.collection_month is not None
    assert record.collection_month.report is not None
    assert record.collection_year is not None
    assert record.collection_year.report is not None

    assert record.collection_day.report.status == ReportScore.SUCCESS
    assert record.collection_month.report.status == ReportScore.SUCCESS
    assert record.collection_year.report.status == ReportScore.SUCCESS

    date = datetime.date(
        int(record.collection_year),
        int(record.collection_month),
        int(record.collection_day),
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


def test_invalid_date():
    """If a date is invalid, all date datapoints should
    get a fail report with an explanation why.
    """
    record = Record.parse_raw(REGISTER_WITH_INVALID_DATE)
    assert record.collection_day is not None
    assert record.collection_day.report is not None
    assert record.collection_month is not None
    assert record.collection_month.report is not None
    assert record.collection_year is not None
    assert record.collection_year.report is not None
    assert record.collection_day.report.status == ReportScore.FAIL
    assert record.collection_month.report.status == ReportScore.FAIL
    assert record.collection_year.report.status == ReportScore.FAIL


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


def test_with_short_year():
    """All years must have four digits"""
    record = Record.parse_raw(REGISTER_WITH_SHORT_YEAR)
    assert record.collection_year is not None
    assert record.collection_year.report is not None
    assert record.collection_year.report.status == ReportScore.FAIL


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
    assert record.latitude is not None
    assert record.latitude.report is not None
    assert record.latitude.report.status == ReportScore.SUCCESS

    assert record.longitude is not None
    assert record.longitude.report is not None
    assert record.longitude.report.status == ReportScore.SUCCESS


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


def test_non_numeric_location():
    """This lat/lon pair is invalid because they are not numeric"""
    record = Record.parse_raw(NON_NUMERIC_INVALID_LOCATION)
    assert record.latitude is not None
    assert record.latitude.report is not None
    assert record.latitude.report.status == ReportScore.FAIL

    assert record.longitude is not None
    assert record.longitude.report is not None
    assert record.longitude.report.status == ReportScore.FAIL


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
    assert record.latitude is not None
    assert record.latitude.report is not None
    assert record.latitude.report.status == ReportScore.FAIL

    assert record.longitude is not None
    assert record.longitude.report is not None
    assert record.longitude.report.status == ReportScore.FAIL


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
    assert record.host_species_ncbi_tax_id is not None
    assert record.host_species_ncbi_tax_id.report is not None
    assert record.host_species_ncbi_tax_id.report.status == ReportScore.SUCCESS

    assert record.detection_target_ncbi_tax_id is not None
    assert record.detection_target_ncbi_tax_id.report is not None
    assert record.detection_target_ncbi_tax_id.report.status == ReportScore.FAIL

    assert record.pathogen_ncbi_tax_id is not None
    assert record.pathogen_ncbi_tax_id.report is not None
    assert record.pathogen_ncbi_tax_id.report.status == ReportScore.FAIL


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
    assert record.mass is not None
    assert record.mass.report is not None
    assert record.mass.report.status == ReportScore.WARNING
    assert record.mass.report.message == "don't modify me"


MINIMAL_RELEASEABLE_REGISTER = """
{
    "register": {
        "rec12345": {
            "Host species": {
                "dataValue": "Vulpes vulpes",
                "modifiedBy": "dev",
                "version": "2"
            },
            "Latitude": {
                "dataValue": "40.0150",
                "modifiedBy": "dev",
                "version": "1679692123"
            },
            "Longitude": {
                "dataValue": "105.2705",
                "modifiedBy": "dev",
                "version": "1679692223"
            },
            "Collection day": {
                "dataValue": "1",
                "modifiedBy": "john",
                "version": "1679692123"
            },
            "Collection month": {
                "dataValue": "1",
                "modifiedBy": "dev",
                "version": "1679692123"
            },
            "Collection year": {
                "dataValue": "2019",
                "modifiedBy": "dev",
                "version": "1679692123"
            },
            "Detection outcome": {
                "dataValue": "positive",
                "modifiedBy": "dev",
                "version": "1679692123"
            },
            "Pathogen": {
                "dataValue": "SARS-CoV-2",
                "modifiedBy": "dev",
                "version": "1679692123"
            },
            "Age": {
                "dataValue": "",
                "modifiedBy": "dev",
                "version": "1679692123",
                "previous": {
                    "dataValue": "1",
                    "modifiedBy": "dev",
                    "version": "1679682123"
                }
            }
        }
    }
}
"""


def test_success_release_report():
    register = Register.parse_raw(MINIMAL_RELEASEABLE_REGISTER)
    report = register.get_release_report()
    assert report is not None
    assert report.release_status is DatasetReleaseStatus.RELEASED
    assert report.success_count == 8
    assert report.warning_count == 0
    assert report.fail_count == 0
    assert report.missing_count == 0
    assert len(report.warning_fields) == 0
    assert len(report.fail_fields) == 0
    assert len(report.missing_fields) == 0

    # A user has deleted "Age" in the interface, so its value is
    # an empty string, so it should be included to keep the history
    # but it should not have a validation report and it should
    # be treated as a missing in the release report. That woun't
    # stop the release because it is not a required field.
    assert register.register_data["rec12345"].age is not None
    assert register.register_data["rec12345"].age.report is None


REGISTER_NOT_READY_TO_RELEASE = """
{
    "register": {
        "rec12345": {
            "Host species": {
                "dataValue": "Vulpes vulpes",
                "modifiedBy": "dev",
                "version": "2"
            },
            "Host species NCBI tax ID": {
                "dataValue": "Vulpes vulpes",
                "modifiedBy": "dev",
                "version": "2"
            },
            "Latitude": {
                "dataValue": "40.0150",
                "modifiedBy": "dev",
                "version": "1679692123"
            },
            "Longitude": {
                "dataValue": "105.2705",
                "modifiedBy": "dev",
                "version": "1679692223"
            },
            "Collection month": {
                "dataValue": "1",
                "modifiedBy": "dev",
                "version": "1679692123"
            },
            "Collection year": {
                "dataValue": "2019",
                "modifiedBy": "dev",
                "version": "1679692123"
            },
            "Pathogen": {
                "dataValue": "SARS-CoV-2",
                "modifiedBy": "dev",
                "version": "1679692123"
            },
            "Detection outcome": {
                "dataValue": "",
                "modifiedBy": "dev",
                "version": "1679692123"
            },
            "Random column": {
                "dataValue": "SARS-CoV-2",
                "modifiedBy": "dev",
                "version": "1679692123"
            }
        }
    }
}
"""


def test_fail_release_report():
    register = Register.parse_raw(REGISTER_NOT_READY_TO_RELEASE)
    report = register.get_release_report()
    assert report is not None
    assert report.release_status is DatasetReleaseStatus.UNRELEASED
    assert report.success_count == 4
    assert report.fail_count == 1
    assert report.warning_count == 1
    assert report.missing_count == 1
    assert report.warning_fields["rec12345"][0] == "Random column"
    assert report.fail_fields["rec12345"][0] == "Host species NCBI tax ID"

    # detection_outcome has a data_value of "" so but it is
    # a required field so it should be in the parsed register
    # but it should not have a report and should show up as a
    # missing field in the release report.
    assert register.register_data["rec12345"].detection_outcome is not None
    assert register.register_data["rec12345"].detection_outcome.report is None

    assert set(report.missing_fields["rec12345"]) == {
        "Collection day",
    }


LEFT_DATAPOINT = """
{
    "Host species": {
        "dataValue": "Most recent",
        "modifiedBy": "dev",
        "version": "4",
        "previous": {
            "dataValue": "Oldest",
            "modifiedBy": "dev",
            "version": "1"
        }
    }
}
"""

RIGHT_DATAPOINT = """
{
    "Host species": {
        "dataValue": "Second most recent",
        "modifiedBy": "dev",
        "version": "3",
        "previous": {
            "dataValue": "Second oldest",
            "modifiedBy": "dev",
            "version": "2"
        }
    }
}
"""


def test_basic_merge_datapoint():
    left = Record.parse_raw(LEFT_DATAPOINT)
    right = Record.parse_raw(RIGHT_DATAPOINT)

    result = Datapoint.merge(left.host_species, right.host_species)

    assert result
    assert result.data_value == "Most recent"
    assert result.previous
    assert result.previous.data_value == "Second most recent"
    assert result.previous.previous
    assert result.previous.previous.data_value == "Second oldest"
    assert result.previous.previous.previous
    assert result.previous.previous.previous.data_value == "Oldest"


def test_merge_datapoint_symmetry():
    left = Record.parse_raw(LEFT_DATAPOINT)
    right = Record.parse_raw(RIGHT_DATAPOINT)

    result = Datapoint.merge(right.host_species, left.host_species)

    assert result
    assert result.data_value == "Most recent"
    assert result.previous
    assert result.previous.data_value == "Second most recent"
    assert result.previous.previous
    assert result.previous.previous.data_value == "Second oldest"
    assert result.previous.previous.previous
    assert result.previous.previous.previous.data_value == "Oldest"


def test_merge_with_none():
    """Merging with none should result in an unchanged datapoint"""
    left = Record.parse_raw(LEFT_DATAPOINT)
    right = Record.construct()

    result = Datapoint.merge(left.host_species, right.host_species)

    assert result
    assert result.previous
    assert result.previous.data_value == "Oldest"


def test_merge_no_previous():
    left = Record.parse_raw(LEFT_DATAPOINT)
    assert left.host_species
    left.host_species.previous = None

    right = Record.parse_raw(RIGHT_DATAPOINT)
    assert right.host_species
    right.host_species.previous = None

    result = Datapoint.merge(left.host_species, right.host_species)

    assert result
    assert result.data_value == "Most recent"
    assert result.previous
    assert result.previous.data_value == "Second most recent"
    assert result.previous.previous == None


def test_merge_with_empty_string():
    """Datapoint merges must preserve datapoints where
    data_value == "" because this means the user has
    intentionally removed the value."""

    left = Record.parse_raw(LEFT_DATAPOINT)

    assert left.host_species
    left.host_species.data_value = ""

    right = Record.parse_raw(RIGHT_DATAPOINT)

    assert right.host_species
    assert right.host_species.previous
    right.host_species.previous.data_value = ""

    result = Datapoint.merge(left.host_species, right.host_species)

    assert result is not None
    assert result.data_value is not None
    assert result.data_value == ""
    assert result.previous is not None
    assert result.previous.previous is not None
    assert result.previous.previous.data_value == ""


DATAPOINT_WITH_FAIL = """
{
    "Host species": {
        "dataValue": "Most recent",
        "modifiedBy": "dev",
        "version": "4",
        "report": {
            "status": "FAIL",
            "message": "Datapoint is not ready to release",
            "data": null
        }
    }
}
"""

CONFLICTING_DATAPOINT_NO_REPORT = """
{
    "Host species": {
        "dataValue": "Most recent",
        "modifiedBy": "dev",
        "version": "4",
        "previous": {
            "dataValue": "Older value",
            "modifiedBy": "dev",
            "version": "1",
            "report": {
                "status": "SUCCESS",
                "message": "Ready to release",
                "data": null
            }
        }
    }
}
"""


def test_merge_with_reports():
    """When merging two datapoints with the same version number,
    preserve the datapoint with a report, without modifying the
    report, duplicating the datapoint, or losing the history."""
    fail = Record.parse_raw(DATAPOINT_WITH_FAIL)
    conflict = Record.parse_raw(CONFLICTING_DATAPOINT_NO_REPORT)

    result = Datapoint.merge(fail.host_species, conflict.host_species)

    assert result
    assert result.data_value == "Most recent"
    assert result.report
    assert result.report.status == ReportScore.FAIL
    assert result.previous
    assert result.previous.report
    assert result.previous.report.status == ReportScore.SUCCESS


LEFT_REGISTER = """
{
    "Host species": {
        "dataValue": "Vulpes vulpes",
        "modifiedBy": "dev",
        "version": "2"
    },
    "Host species NCBI tax ID": {
        "dataValue": "Vulpes vulpes",
        "modifiedBy": "dev",
        "version": "2"
    },
    "Latitude": {
        "dataValue": "40.0150",
        "modifiedBy": "dev",
        "version": "1679692123"
    },
    "Longitude": {
        "dataValue": "105.2705",
        "modifiedBy": "dev",
        "version": "1679692223"
    },
    "Collection month": {
        "dataValue": "1",
        "modifiedBy": "dev",
        "version": "1679692123"
    },
    "Collection year": {
        "dataValue": "2019",
        "modifiedBy": "dev",
        "version": "1679692123"
    },
    "Pathogen": {
        "dataValue": "SARS-CoV-2",
        "modifiedBy": "dev",
        "version": "1679692123"
    },
    "Detection outcome": {
        "dataValue": "",
        "modifiedBy": "dev",
        "version": "2679692123"
    },
    "Random column": {
        "dataValue": "SARS-CoV-2",
        "modifiedBy": "dev",
        "version": "1679692123"
}
}
"""

RIGHT_REGISTER = """
{
    "Host species": {
        "dataValue": "Old host species",
        "modifiedBy": "dev",
        "version": "1"
    },
    "Host species NCBI tax ID": {
        "dataValue": "Vulpes vulpes",
        "modifiedBy": "dev",
        "version": "2"
    },
    "Latitude": {
        "dataValue": "40.0150",
        "modifiedBy": "dev",
        "version": "1679692123"
    },
    "Longitude": {
        "dataValue": "105.2705",
        "modifiedBy": "dev",
        "version": "1679692223"
    },
    "Collection month": {
        "dataValue": "1",
        "modifiedBy": "dev",
        "version": "1679692123"
    },
    "Collection year": {
        "dataValue": "2019",
        "modifiedBy": "dev",
        "version": "1679692123"
    },
    "Pathogen": {
        "dataValue": "SARS-CoV-2",
        "modifiedBy": "dev",
        "version": "1679692123"
    },
    "Detection outcome": {
        "dataValue": "Old detection outcome",
        "modifiedBy": "dev",
        "version": "1676692123"
    },
    "Random column": {
        "dataValue": "SARS-CoV-2",
        "modifiedBy": "dev",
        "version": "1679692123"
    }
}
"""


def test_merge_register():
    left = Record.parse_raw(LEFT_REGISTER)
    right = Record.parse_raw(RIGHT_REGISTER)

    merged = Record.merge(left, right)

    assert merged
    assert merged.host_species
    assert str(merged.host_species) == "Vulpes vulpes"
    assert merged.host_species.previous
    assert merged.host_species.previous.data_value == "Old host species"
    assert merged.host_species.previous.previous == None

    assert merged.detection_outcome is not None
    assert str(merged.detection_outcome) == ""
    assert merged.detection_outcome.previous
    assert str(merged.detection_outcome.previous) == "Old detection outcome"

    assert merged.detection_outcome.previous.report
    assert merged.detection_outcome.previous.report.status == ReportScore.FAIL


def test_merge_register_symmetry():
    left = Record.parse_raw(LEFT_REGISTER)
    right = Record.parse_raw(RIGHT_REGISTER)

    merged = Record.merge(right, left)

    assert merged
    assert merged.host_species
    assert str(merged.host_species) == "Vulpes vulpes"
    assert merged.host_species.previous
    assert merged.host_species.previous.data_value == "Old host species"
    assert merged.host_species.previous.previous == None

    assert merged.detection_outcome is not None
    assert str(merged.detection_outcome) == ""
    assert merged.detection_outcome.previous
    assert str(merged.detection_outcome.previous) == "Old detection outcome"

    assert merged.detection_outcome.previous.report
    assert merged.detection_outcome.previous.report.status == ReportScore.FAIL
