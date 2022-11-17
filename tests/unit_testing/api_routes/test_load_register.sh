#!/bin/bash

if [[ -z "$1" ]];
    then
        exit 1
    else
        API=$1
fi

curl $API/load-register --data-raw '{"researcherID" : "dev", "datasetID" : "testing"}' -o load_register_test_output.json