import boto3
import psycopg2
from format import format_response

RDS = boto3.client("rds")

# This function might need to be async as per AWS documentation:
# You must ensure that you have static or previously resolved credentials if you call this method synchronously (with no callback),
# # otherwise it may not properly sign the request. If you cannot guarantee this (you are using an asynchronous credential provider,
# # i.e., EC2 IAM roles), you should always call this method with an asynchronous callback.

# IAM authentication is required, hence IAM token must be created
def proxy_token(parameters):
    token = RDS.generate_db_auth_token(
        DBHostname=parameters["hostname"],
        Port=parameters["port"],
        DBUsername=parameters["username"],
        Region=parameters["region"],
    )

    return token


def database(parameters):
    token = proxy_token(parameters)

    try:
        connection = psycopg2.connect(
            host=parameters["hostname"],
            user=parameters["username"],
            password=token,
            db=parameters["database"],
        )

        return connection

    except psycopg2.Error as e:
        format_response(403, e)


def lambda_handler(event, _):
    connection = database(something)
    cursor = connection.cursor()
    cursor.execute(query)
    return format_response(200, "")
