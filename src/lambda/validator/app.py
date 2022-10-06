import json
from auth import check_auth
from format import format_response


def validator(name):
    module = getattr(__import__(f"validators.{name}"), name)
    return getattr(module, name)


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    # Placeholder check user authorization
    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    validation_report = {}

    for recordid, record in post_data["rows"].items():
        record_ = {}
        for column, value in record:
            column_ = column.replace(" ", "").capitalize()
            try:
                Validator = validator(column_)
                report = Validator(value).run_validation()
                record_[column] = report
            except Exception:
                continue

        validation_report[recordid] = record_

    return format_response(200, validation_report)
