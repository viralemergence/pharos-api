import logging
import boto3


def lambda_handler(event, context) -> None:
    return


def storeDynamodb(user:str, file:str) -> bool:
    
    try:
        pass
    except:
        pass
    
    return True

def uploadS3(json, bucket) -> bool:

    client = boto3.client("s3")

    try:
        response = client.upload_file(json, bucket)
    except ClientError:
        return False
    except FileNotFoundError:
        return False
    except NoCredentialsError:
        return False

    return True