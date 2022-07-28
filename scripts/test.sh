#!/bin/bash

api="$PHAROS_API"

# review: https://wa769evyjf.execute-api.us-east-2.amazonaws.com/Prod

# parse params
while [[ "$#" > 0 ]];
  do case $1 in
    -a|--api) api=$2; shift;;
    *) echo "Unknown parameter passed: $1"; exit 1 ;;
  esac
  shift
done

echo "$api/save-register"

curl \
 -X POST "$api/save-register" \
  -H 'content-type: application/json' \
  --data '{
   "researcherID":"dev",
   "datasetID":"1658761515478",
   "data":{
      "register":{
         "exampleRecordID":{
            "DetectionID":{
               "displayValue":"test",
               "dataValue":"1",
               "version":"0"
            },
            "SampleID":{
               "displayValue":"save this",
               "dataValue":"save this",
               "version":"0",
               "modifiedBy":"dev"
            },
            "DetectionMethod":{
               "displayValue":"sdfsdf",
               "dataValue":"sdfsdf",
               "version":"2",
               "modifiedBy":"dev",
               "previous":{
                  "displayValue":"",
                  "dataValue":"",
                  "version":"0"
               }
            },
            "DetectionOutcome":{
               "displayValue":"",
               "dataValue":"",
               "version":"0"
            },
            "DetectionComments":{
               "displayValue":"",
               "dataValue":"",
               "version":"0"
            },
            "PathogenTaxID":{
               "displayValue":"",
               "dataValue":"",
               "version":"0"
            },
            "GenbankAccession":{
               "displayValue":"",
               "dataValue":"",
               "version":"0"
            },
            "SRAA ccession":{
               "displayValue":"",
               "dataValue":"",
               "version":"0"
            },
            "GISAIDAccession":{
               "displayValue":"",
               "dataValue":"",
               "version":"0"
            },
            "GBIFIdentifier":{
               "displayValue":"",
               "dataValue":"",
               "version":"0"
            }
         }
      },
      "versions":[
         {
            "date":"Mon, 25 Jul 2022 15:05:35 GMT",
            "name":"Mon, 25 Jul 2022 15:05:35 GMT"
         },
         {
            "date":"Mon, 25 Jul 2022 16:06:28 GMT",
            "name":"Mon, 25 Jul 2022 16:06:28 GMT"
         }
      ]
   }
}'
