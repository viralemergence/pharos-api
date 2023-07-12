from published_records import get_query
from .fixture import ENGINE, mock_data


def check(params, expected_record_count):
    assert len(get_query(ENGINE, params).all()) == expected_record_count


def test_no_filters(mock_data):
    check({}, 400)


def test_filter_by_pharos_id(mock_data):
    check({"pharos_id": "project0-dataset0-rec0"}, 1)
    check({"pharos_id": ["project0-dataset0-rec0", "project1-dataset1-rec1"]}, 2)
    check(
        {"pharos_id": [f"project0-dataset0-rec{num}" for num in range(25)]},
        25,
    )


def test_filter_by_host_species(mock_data):
    check({"host_species": "host1"}, 300)
    check({"host_species": "host2"}, 60)
    check({"host_species": "host3"}, 40)
    check({"host_species": ["host1", "host2"]}, 360)
    check({"host_species": ["host2", "host3"]}, 100)
    check({"host_species": ["host1", "host3"]}, 340)


def test_filter_by_project_name(mock_data):
    check({"project_name": "Project Zero"}, 200)
    # case insensitive
    check({"project_name": "project zero"}, 200)
    # the whole string must match
    check({"project_name": "Zero"}, 0)


def test_filter_by_pathogen(mock_data):
    check({"pathogen": "path1"}, 220)
    check({"pathogen": "path2"}, 60)
    check({"pathogen": "path3"}, 120)
    check({"pathogen": ["path1", "path2"]}, 280)
    check({"pathogen": ["path2", "path3"]}, 180)
    check({"pathogen": ["path1", "path3"]}, 340)


def test_filter_by_project_name_and_host_species(mock_data):
    check({"project_name": "Project Zero", "host_species": "host1"}, 150)
    check({"project_name": "Project Zero", "host_species": "host2"}, 30)
    check({"project_name": "Project Zero", "host_species": "host3"}, 20)
    check({"project_name": "Project Zero", "host_species": ["host1", "host2"]}, 180)
    check({"project_name": "Project Zero", "host_species": ["host2", "host3"]}, 50)
    check({"project_name": "Project Zero", "host_species": ["host1", "host3"]}, 170)


def test_filter_by_host_species_and_pathogen(mock_data):
    check({"host_species": ["host1", "host2"], "pathogen": ["path2", "path3"]}, 140)


def test_filter_by_collection_date(mock_data):
    check({"collection_end_date": "2024-1-2"}, 200)
    check({"collection_start_date": "2024-1-2"}, 200)


def test_filter_by_collection_date_and_host_species(mock_data):
    check({"collection_start_date": "2023-12-31", "host_species": "host1"}, 200)
    check({"collection_end_date": "2023-1-2", "host_species": "host2"}, 0)


def test_filter_by_researcher_name(mock_data):
    check({"researcher": "Researcher Zero"}, 200)
    check({"researcher": "Researcher One"}, 200)
    check({"researcher": "Researcher Two"}, 200)
    check({"researcher": "Researcher Three"}, 200)


def test_filter_by_researcher_name_and_project_name(mock_data):
    check({"researcher": "Researcher Zero", "project_name": "Project Zero"}, 200)
    check({"researcher": "Researcher Zero", "project_name": "Project One"}, 0)
