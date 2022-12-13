import json
import requests
import pytest

# Load register with one record
with open("./tests/integration_tests/record.json") as file:
    initial_record = json.load(file)


def test_save_register_one_record():
    """Save a register with only one record."""
    pass


def test_load_register_one_record():
    """Load a register with one record."""
    pass


def test_publish_register_one_record():
    """Publish a register with one record."""
    pass


# Load register with multiple records
with open("./tests/integration_tests/register.json") as file:
    initial_record = json.load(file)


def test_save_register_multiple_record():
    """Save a register with multiple records."""
    pass


def test_load_register_multiple_record():
    """Load a register with multiple records."""
    pass


def test_publish_register_multiple_record():
    """Publish a regster with multiple records"""
    pass


def test_retrieve_database():
    """Retrieve all the records in the Records table."""
    pass
