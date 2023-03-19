import datetime
import pytest
from register import Record, ReportScore

valid_record = """
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
    record = Record.parse_raw(valid_record)
    assert record.Host_species is not None
    assert record.Host_species.dataValue == "Vulpes vulpes"
    assert record.Host_species.report is not None
    assert record.Host_species.report.status == ReportScore.success
    assert record.Host_species.previous is not None
    assert record.Host_species.previous.dataValue == "Previous Data Value"
    assert record.Host_species.version == 2
    assert record.Host_species.previous.version == 1


def test_valid_coersion():
    record = Record.parse_raw(valid_record)
    assert record.Age is not None
    assert record.Age.dataValue == "10"
    assert float(record.Age) == 10.0
    assert str(record.Age) == "10"
    assert int(record.Age) == 10
    assert record.Age.nonzero_int() == 10
    assert record.Age.isnumeric() is True


def test_coersion_exception():
    record = Record.parse_raw(valid_record)
    assert record.Host_species is not None
    assert record.Age is not None
    assert record.Age.previous is not None
    assert record.Length is not None
    assert record.Length.report is not None
    assert record.Length.report.status == ReportScore.fail

    with pytest.raises(ValueError):
        int(record.Host_species)

    with pytest.raises(ValueError):
        float(record.Host_species)

    with pytest.raises(ValueError):
        record.Host_species.nonzero_int()

    with pytest.raises(ValueError):
        record.Age.previous.nonzero_int()

    assert record.Host_species.isnumeric() is False


unknown_datapoint = """
{
    "Unknown datapoint": {
        "dataValue": "something odd",
        "modifiedBy": "dev",
        "version": "1"
    }
}
"""


def test_unknown_datapoint():
    record = Record.parse_raw(unknown_datapoint)
    datapoint = getattr(record, "Unknown datapoint")
    assert datapoint is not None
    assert datapoint.report is not None
    assert datapoint.report.status == ReportScore.warning


datapoint_illegal_additional_keys = """
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
    with pytest.raises(ValueError):
        Record.parse_raw(datapoint_illegal_additional_keys)


missing_required_value = """
{
    "Host species": {
        "dataValue": "Vulpes vulpes",
        "version": "2"
    }
}
"""


def test_missing_required_value():
    with pytest.raises(ValueError):
        Record.parse_raw(missing_required_value)


register_with_partial_date = """
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
    record = Record.parse_raw(register_with_partial_date)
    assert record.Collection_day is not None
    assert record.Collection_day.report is None
    assert record.Collection_year is not None
    assert record.Collection_year.report is None


register_with_date = """
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
    record = Record.parse_raw(register_with_date)
    assert record.Collection_day is not None
    assert record.Collection_day.report is not None
    assert record.Collection_month is not None
    assert record.Collection_month.report is not None
    assert record.Collection_year is not None
    assert record.Collection_year.report is not None

    assert record.Collection_day.report.status == ReportScore.success
    assert record.Collection_month.report.status == ReportScore.success
    assert record.Collection_year.report.status == ReportScore.success

    date = datetime.date(
        int(record.Collection_year),
        int(record.Collection_month),
        int(record.Collection_day),
    )

    assert date == datetime.date(2022, 1, 1)


register_with_invalid_date = """
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
    record = Record.parse_raw(register_with_invalid_date)
    assert record.Collection_day is not None
    assert record.Collection_day.report is not None
    assert record.Collection_month is not None
    assert record.Collection_month.report is not None
    assert record.Collection_year is not None
    assert record.Collection_year.report is not None

    print(record.Collection_day.report.status)

    assert record.Collection_day.report.status == ReportScore.fail
    assert record.Collection_month.report.status == ReportScore.fail
    assert record.Collection_year.report.status == ReportScore.fail


register_with_short_year = """
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
    record = Record.parse_raw(register_with_short_year)
    assert record.Collection_year is not None
    assert record.Collection_year.report is not None
    assert record.Collection_year.report.status == ReportScore.fail


valid_location = """
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
    record = Record.parse_raw(valid_location)
    assert record.Latitude is not None
    assert record.Latitude.report is not None
    assert record.Latitude.report.status == ReportScore.success

    assert record.Longitude is not None
    assert record.Longitude.report is not None
    assert record.Longitude.report.status == ReportScore.success


non_numeric_invalid_location = """
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
    record = Record.parse_raw(non_numeric_invalid_location)
    assert record.Latitude is not None
    assert record.Latitude.report is not None
    assert record.Latitude.report.status == ReportScore.fail

    assert record.Longitude is not None
    assert record.Longitude.report is not None
    assert record.Longitude.report.status == ReportScore.fail


numeric_invalid_location = """
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
    record = Record.parse_raw(numeric_invalid_location)
    assert record.Latitude is not None
    assert record.Latitude.report is not None
    assert record.Latitude.report.status == ReportScore.fail

    assert record.Longitude is not None
    assert record.Longitude.report is not None
    assert record.Longitude.report.status == ReportScore.fail


ncbi_tax_ids = """
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
    record = Record.parse_raw(ncbi_tax_ids)
    assert record.Host_species_NCBI_tax_ID is not None
    assert record.Host_species_NCBI_tax_ID.report is not None
    assert record.Host_species_NCBI_tax_ID.report.status == ReportScore.success

    assert record.Detection_target_NCBI_tax_ID is not None
    assert record.Detection_target_NCBI_tax_ID.report is not None
    assert record.Detection_target_NCBI_tax_ID.report.status == ReportScore.fail

    assert record.Pathogen_NCBI_tax_ID is not None
    assert record.Pathogen_NCBI_tax_ID.report is not None
    assert record.Pathogen_NCBI_tax_ID.report.status == ReportScore.fail


existing_warning = """
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
    record = Record.parse_raw(existing_warning)
    assert record.Mass is not None
    assert record.Mass.report is not None
    assert record.Mass.report.status == ReportScore.warning
    assert record.Mass.report.message == "don't modify me"
