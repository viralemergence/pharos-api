import json
import os

CORS_ALLOW = os.environ["CORS_ALLOW"]


def format_response(code, body):
    return {
        "statusCode": code,
        "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
        "body": json.dumps(body),
    }
