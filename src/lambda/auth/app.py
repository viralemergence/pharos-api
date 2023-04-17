import json
from auth import check_auth
from format import format_response


def lambda_handler(event, _):
    post_data = json.loads(event.get("body", "{}"))
    user = check_auth(post_data["researcherID"])

    if not user:
        return format_response(401, {"message": "Unauthorized"})

    return format_response(200, user.json(by_alias=True), preformatted=True)
