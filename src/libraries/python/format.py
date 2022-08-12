import os
import json
from decimal import Decimal

CORS_ALLOW = os.environ["CORS_ALLOW"]


class SetEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        if isinstance(o, set):
            return list(o)
        return json.JSONEncoder.default(self, o)


def format_response(code, body):
    try:
        body_string = json.dumps(body, cls=SetEncoder)

    except TypeError:
        body_string = str(body)

    return {
        "statusCode": code,
        "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
        "body": body_string,
    }
