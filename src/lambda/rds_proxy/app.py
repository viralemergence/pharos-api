import boto3
import psycopg2
from format import format_response

RDS = boto3.client("rds")


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
