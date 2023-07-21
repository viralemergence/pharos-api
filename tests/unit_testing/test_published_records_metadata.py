# pylint: disable=redefined-outer-name
# pylint: disable=unused-argument
# pylint: disable=unused-import

from fixture import ENGINE, mock_data
from published_records_metadata import get_fields


def test_get_fields(mock_data):
    fields = get_fields(ENGINE)
    assert fields["project_name"]["options"] == ["Project One", "Project Zero"]
    assert fields["host_species"]["options"] == ["host1", "host2", "host3"]
    assert fields["pathogen"]["options"] == ["path1", "path2", "path3"]
    assert fields["detection_target"]["options"] == ["target1", "target2", "target3"]
    assert fields["detection_outcome"]["options"] == [
        "inconclusive",
        "negative",
        "positive",
    ]
    assert list(fields.keys()) == [
        "project_name",
        "researcher_name",
        "host_species",
        "detection_target",
        "detection_outcome",
        "pathogen",
        "collection_start_date",
        "collection_end_date",
    ]
