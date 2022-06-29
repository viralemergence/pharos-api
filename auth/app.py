import json
import os

# import requests

CORS_ALLOW = os.environ["CORS_ALLOW"]


def lambda_handler(event, context):

    print("\n\n\nfunction called")

    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    post_data = json.loads(event.get("body", "{}"))

    # Fake auth response for valid user
    if post_data["id"] == "1234":
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": json.dumps(
                {
                    "token": "12-309oij13120-8254",
                    "username": "dev user",
                    "email": "devs@talusanalytics.com",
                    "researcherID": "5431",
                }
            ),
        }

    return {
        "statusCode": 401,
        "headers": {
            "Access-Control-Allow-Origin": CORS_ALLOW,
        },
        "body": json.dumps(
            {
                "message": "User does not exist",
            }
        ),
    }
