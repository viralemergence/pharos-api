from decimal import Decimal
import json
import os
import boto3
from boto3.dynamodb.conditions import Key

from auth import check_auth
from format import format_response


DYNAMODB = boto3.resource("dynamodb")
CORS_ALLOW = os.environ["CORS_ALLOW"]
DATASETS_TABLE = DYNAMODB.Table(os.environ["DATASETS_TABLE_NAME"])
PROJECTS_TABLE = DYNAMODB.Table(os.environ["PROJECTS_TABLE_NAME"])


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return json.JSONEncoder.default(self, o)


def lambda_handler(event, _):
    """
    List datasets per project
    """

    post_data = json.loads(event.get("body", "{}"))

    authorized = check_auth(post_data["researcherID"])
    if not authorized:
        return format_response(403, "Not Authorized")

    try:
        # Get project
        project = PROJECTS_TABLE.get_item(Key={"projectID": post_data["projectID"]})

        datasets = {}
        # From the list of datasetIDs query for the records that contain '_meta' as sort key
        if isinstance(project["Item"]["datasetIDs"], list):
            for datasetid in project["Item"]["datasetIDs"]:
                query = DATASETS_TABLE.query(
                    KeyConditionExpression=Key("datasetID").eq(datasetid)
                    & Key("recordID").eq("_meta")
                )
                # we're assuming we only want the first result
                # from the query since we know that there
                # should only be one recordID:_meta PK:SK pair
                dataset = query["Items"][0]
                # Unpack query and append

                datasets[dataset["datasetID"]] = {**dataset, "status": "Saved"}

        return format_response(200, datasets)

    except Exception as e:  # pylint: disable=broad-except
        return format_response(403, e)
