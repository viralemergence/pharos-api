#!/bin/bash

api=$PHAROS_API

# review: https://wa769evyjf.execute-api.us-east-2.amazonaws.com/Prod

# parse params
while [[ "$#" > 0 ]];
  do case $1 in
    -a|--api) api=$2; shift;;
    *) echo "Unknown parameter passed: $1"; exit 1 ;;
  esac
  shift
done


curl -X POST $api/create-user \
  -d '{"researcherID": "resdev" name":"resdev", "email":"developer@example.com", "organization":"Developer organization"}'\

