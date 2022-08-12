import json
import os

CORS_ALLOW = os.environ["CORS_ALLOW"]

class SetEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, set):
            return list(o)
        return json.JSONEncoder.default(self, o)



def format_response(code, body):
    return {
        "statusCode": code,
        "headers": {"Access-Control-Allow-Origin": CORS_ALLOW},
        "body": json.dumps(body, cls=SetEncoder),
    }
