
if [[ -z "$1" ]];
    then
        echo "Define an api"
        exit 1
    else
        api=$1
fi

curl $api/list-datasets \ 
    --data-raw '{"researcherID" : "dev", "projectID" : "prjGDcNl5V9d0"}';

# [{"datasetID": "testing", "recordID": "_meta", "record": {"something": "some something"}}]%