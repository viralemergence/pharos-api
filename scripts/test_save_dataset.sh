
if [[ -z "$1" ]];
    then
        echo "Define an api"
        exit 1
    else
        api=$1
fi


curl $api/save-dataset \
  --data-raw '{"projectID" : "prjGDcNl5V9d0", "name":"test", "datasetID":"setQr8GFTxd3z", "researcherID":"dev", "samples_taken":"0", "detection_run":"0", "versions":[ ], "highestVersion":0, "lastUpdated":"2022-08-30T18:23:15.260Z", "activeVersion":0, "releaseStatus":"Unreleased" }'

  # "Succesful Upload"%