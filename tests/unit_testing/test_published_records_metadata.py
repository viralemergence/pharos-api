# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
# pylint: disable=unused-import

from fixture import ENGINE, mock_data
from published_records_metadata import get_possible_filters


def test_get_possible_filters(mock_data):
    filters = get_possible_filters(ENGINE)
    assert filters["project_name"]["options"] == ["Project One", "Project Zero"]
    assert filters["host_species"]["options"] == ["host1", "host2", "host3"]
    assert filters["pathogen"]["options"] == ["path1", "path2", "path3"]
    assert filters["detection_target"]["options"] == ["target1", "target2", "target3"]
    assert filters["detection_outcome"]["options"] == [
        "inconclusive",
        "negative",
        "positive",
    ]
    assert filters["collection_start_date"]["earliestDateInDatabase"] == "2023-01-01"
    assert filters["collection_start_date"]["latestDateInDatabase"] == "2026-01-01"
    assert filters["collection_end_date"]["earliestDateInDatabase"] == "2023-01-01"
    assert filters["collection_end_date"]["latestDateInDatabase"] == "2026-01-01"
    assert list(filters.keys()) == [
        "project_name",
        "researcher_name",
        "host_species",
        "detection_target",
        "detection_outcome",
        "pathogen",
        "collection_start_date",
        "collection_end_date",
    ]
