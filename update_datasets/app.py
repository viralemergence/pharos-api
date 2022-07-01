
CLIENT = boto3.resource('dynamodb')
DATASET_TABLE = CLIENT.Table(os.environ["DATASET_TABLE_NAME"])

def lambda_handler(request, dataset):
    researcherid = request["researcherID"]
    datasetid = request["datasetID"]

    response = DATASET_TABLE.update_item(
        Key = {"researcherID" : researcherid, "datasetID": datasetid},
        UpdateExpression = {"append dataset." + datasetid + ".versions=:d" }
        ExpressionAttributeValues={":d" : dataset}
    )

    return { # Change essage to the user
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": CORS_ALLOW,
            },
            "body": {

            }
        }