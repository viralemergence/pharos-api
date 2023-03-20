from auth import check_auth


def test_check_auth():
    authorized = check_auth("dev")
    assert authorized is False
