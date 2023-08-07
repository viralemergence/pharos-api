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


def format_response(code, body, preformatted=False):
    if not preformatted:
        try:
            body_string = json.dumps(body, cls=SetEncoder)

        except TypeError:
            body_string = str(body)
    else:
        body_string = body

    print(
        {
            "statusCode": code,
            "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
            "body": body_string,
        }
    )
    return {
        "statusCode": code,
        "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
        "body": body_string,
    }
