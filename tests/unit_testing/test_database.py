"""Basic tests for transforming records into database tests"""

from devtools import debug
from register import Record

from models2 import PublishedRecord, Researcher

RESEARCHER = Researcher(
    researcher_id="dev",
    first_name="John",
    last_name="Doe",
)


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
        "dataValue": "23",
        "modifiedBy": "dev",
        "version": "2"
    }
}
"""


def test_transform_record():
    record = Record.parse_raw(VALID_RECORD)
    debug(record)

    record_not_none = {k: v for k, v in record.__dict__.items() if v is not None}

    debug(record_not_none)

    test = PublishedRecord(**record_not_none)
    # assert test.animal_id == record.animal_id
    # assert test.host_species == record.host_species
    debug(test)

    # assert test.attributions[0].researcher == RESEARCHER
    # assert test.attributions[0].version == record.version


if __name__ == "__main__":
    test_transform_record()
