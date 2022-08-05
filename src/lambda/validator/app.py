import json
from auth import check_auth  # pylint: disable=no-name-in-module
from format import format_response  # pylint: disable=import-error


def validate(name, payload):
    module = getattr(__import__(f"validators.{name}"), name)
    return module.test(payload)


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    # Placeholder check user authorization
    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    report = []
    for row in iter(post_data["rows"]):
        for column, value in row:
            try:
                report.append(validate(column, value))
            except KeyError:
                report.append({"pass": False, "message": "Column not found"})

    return format_response(200, report)
