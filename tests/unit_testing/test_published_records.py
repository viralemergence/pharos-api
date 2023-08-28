# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
# pylint: disable=unused-import

from types import SimpleNamespace
from typing import Dict, List
from sqlalchemy.orm import Session
from published_records import (
    QueryStringParameters,
    format_response_rows,
    query_records,
    get_published_records_response,
)
from models import PublishedRecord
from fixture import ENGINE, mock_data


def check(params, expected_record_count):
    params_obj = QueryStringParameters(page=1, pageSize=50, **params)
    with Session(ENGINE) as session:
        (query, _) = query_records(session, params_obj)
        assert len(query.all()) == expected_record_count


def test_total_number_of_records(mock_data):
    """Verify a key aspect of the mock data"""
    with Session(ENGINE) as session:
        records = session.query(PublishedRecord).all()
        assert len(records) == 400


def test_no_filters(mock_data):
    check({}, 400)


def test_filter_by_pharos_id(mock_data):
    check({"pharos_id": ["project0-dataset0-rec0"]}, 1)
    check({"pharos_id": ["project0-dataset0-rec0", "project1-dataset1-rec1"]}, 2)
    check(
        {"pharos_id": [f"project0-dataset0-rec{num}" for num in range(25)]},
        25,
    )


def test_filter_by_host_species(mock_data):
    check({"host_species": ["host1"]}, 300)
    check({"host_species": ["host2"]}, 60)
    check({"host_species": ["host3"]}, 40)
    check({"host_species": ["host1", "host2"]}, 360)
    check({"host_species": ["host2", "host3"]}, 100)
    check({"host_species": ["host1", "host3"]}, 340)


def test_filter_by_project_name(mock_data):
    check({"project_name": ["Project Zero"]}, 200)
    # case insensitive
    check({"project_name": ["project zero"]}, 200)
    # the whole string must match
    check({"project_name": ["Zero"]}, 0)


def test_filter_by_pathogen(mock_data):
    check({"pathogen": ["path1"]}, 220)
    check({"pathogen": ["path2"]}, 60)
    check({"pathogen": ["path3"]}, 120)
    check({"pathogen": ["path1", "path2"]}, 280)
    check({"pathogen": ["path2", "path3"]}, 180)
    check({"pathogen": ["path1", "path3"]}, 340)


def test_filter_by_project_name_and_host_species(mock_data):
    check({"project_name": ["Project Zero"], "host_species": ["host1"]}, 150)
    check({"project_name": ["Project Zero"], "host_species": ["host2"]}, 30)
    check({"project_name": ["Project Zero"], "host_species": ["host3"]}, 20)
    check({"project_name": ["Project Zero"], "host_species": ["host1", "host2"]}, 180)
    check({"project_name": ["Project Zero"], "host_species": ["host2", "host3"]}, 50)
    check({"project_name": ["Project Zero"], "host_species": ["host1", "host3"]}, 170)


def test_filter_by_host_species_and_pathogen(mock_data):
    check({"host_species": ["host1", "host2"], "pathogen": ["path2", "path3"]}, 140)


def test_filter_by_collection_date(mock_data):
    check({"collection_end_date": "2024-1-2"}, 200)
    check({"collection_start_date": "2024-1-2"}, 200)


def test_filter_by_collection_date_and_host_species(mock_data):
    check({"collection_start_date": "2023-12-31", "host_species": ["host1"]}, 200)
    check({"collection_end_date": "2023-1-2", "host_species": ["host2"]}, 0)


def test_filter_by_researcher_name(mock_data):
    check({"researcher_name": ["Researcher Zero"]}, 200)
    check({"researcher_name": ["Researcher One"]}, 200)
    check({"researcher_name": ["Researcher Two"]}, 200)
    check({"researcher_name": ["Researcher Three"]}, 200)


def test_filter_by_researcher_name_and_project_name(mock_data):
    check(
        {"researcher_name": ["Researcher Zero"], "project_name": ["Project Zero"]}, 200
    )
    check({"researcher_name": ["Researcher Zero"], "project_name": ["Project One"]}, 0)


def test_filter_by_dataset_id(mock_data):
    check({"dataset_id": "dataset0"}, 200)
    check({"dataset_id": "dataset1"}, 200)


def test_format_response_rows(mock_data):
    with Session(ENGINE) as session:
        (query, _) = query_records(session, {})
        rows = query.limit(50).offset(0).all()
    formatted_rows = format_response_rows(rows, 0)
    assert formatted_rows[0] == {
        "pharosID": "project0-dataset0-rec0",
        "rowNumber": 1,
        "Project name": "Project Zero",
        "Researcher": [
            {"name": "Researcher One", "researcherID": "researcher1"},
            {"name": "Researcher Zero", "researcherID": "researcher0"},
        ],
        "Collection date": "2023-01-01",
        "Latitude": -105.2705,
        "Longitude": 40.015,
        "Sample ID": None,
        "Animal ID": None,
        "Host species": "host1",
        "Host species NCBI tax ID": None,
        "Collection method or tissue": None,
        "Detection method": None,
        "Primer sequence": None,
        "Primer citation": None,
        "Detection target": "target1",
        "Detection target NCBI tax ID": None,
        "Detection outcome": "positive",
        "Detection measurement": None,
        "Detection measurement units": None,
        "Pathogen": "path1",
        "Pathogen NCBI tax ID": None,
        "GenBank accession": None,
        "Detection comments": None,
        "Organism sex": None,
        "Dead or alive": None,
        "Health notes": None,
        "Life stage": None,
        "Age": None,
        "Mass": None,
        "Length": None,
        "Spatial uncertainty": None,
    }


def test_get_published_records_response_without_filters(mock_data):
    params = QueryStringParameters(pageSize=50, page=1, **{})
    response = get_published_records_response(ENGINE, params)
    assert len(response["publishedRecords"]) == 50
    assert response["isLastPage"] is False
    assert response["recordCount"] == 400
    assert response["matchingRecordCount"] == 400


def test_get_published_records_response_with_filters(mock_data):
    params = QueryStringParameters(
        pageSize=50, page=1, researcher_name=["Researcher Zero"], **{}
    )
    response = get_published_records_response(ENGINE, params)
    assert len(response["publishedRecords"]) == 50
    assert response["isLastPage"] is False
    assert response["recordCount"] == 400
    assert response["matchingRecordCount"] == 200
