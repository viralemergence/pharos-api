"""
NOTE:

    This function is significantly misnamed; this function
    is used to load updated metadata about the user, and
    is not part of authentication at all.

"""
from auth import check_auth
from format import format_response


def lambda_handler(event, _):
    user = check_auth(event)

    if not user:
        return format_response(401, {"message": "Unauthorized"})

    return format_response(200, user.json(by_alias=True), preformatted=True)
