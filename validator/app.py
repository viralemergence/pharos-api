import os
import json
import boto3

DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]
USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])


def validate(name, payload):
    module = getattr(__import__(f"validators.{name}"), name)
    return module.test(payload)


def lambda_handler(event, _):

    post_data = json.loads(event.get("body", "{}"))

    # # Verify researcher id is in USERS_TABLE - Authentication FAKE
    try:
        users_response = USERS_TABLE.get_item(
            Key={"researcherID": post_data["researcherID"]}
        )

        # Exit if user is not in the database.
        if "Item" not in users_response:
            return {
                "statusCode": 403,
                "headers": {
                    "Access-Control-Allow-Origin": CORS_ALLOW,
                },
                "body": json.dumps({"message": "User does not exist"}),
            }
    except Exception as e:  # pylint: disable=broad-except
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps({"message": str(e)}),
        }

    report = []
    for row in iter(post_data["rows"]):
        for column, value in row:
            try:
                report.append(validate(column, value))
            except KeyError:
                report.append({"pass": False, "message": "Column not found"})

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": CORS_ALLOW,
        },
        "body": json.dumps(report),
    }
