
CLIENT = boto3.resource('dynamodb')
DATASET_TABLE = CLIENT.Table(os.environ["DATASET_TABLE_NAME"])

def lambda_handler(request):
    researcherid = request["researcherID"] # Something like that

    response = DATASET_TABLE.get_item(
        Key = {'researcherID' : researcherid}
        )

    return { # Change essage to the user
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": response["item"] # I think this is an iterable
        }