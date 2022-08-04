import os
import boto3

DYNAMODB = boto3.resource("dynamodb")
USERS_TABLE = DYNAMODB.Table(os.environ["USERS_TABLE_NAME"])


def check_auth(researcherID):
    # Verify researcher id is in USERS_TABLE - Authentication FAKE
    try:
        users_response = USERS_TABLE.get_item(Key={"researcherID": researcherID})
        # Exit if user is not in the database.
        if "Item" not in users_response:
            return False

    except Exception:  # pylint: disable=broad-except
        return False

    return True
