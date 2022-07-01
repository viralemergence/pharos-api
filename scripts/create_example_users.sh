#!/bin/bash

api='http://localhost:3000'

# review: https://wa769evyjf.execute-api.us-east-2.amazonaws.com/Prod

# parse params
while [[ "$#" > 0 ]];
  do case $1 in
    -a|--api) api=$2; shift;;
    *) echo "Unknown parameter passed: $1"; exit 1 ;;
  esac
  shift
done


curl -d \
  '{"name":"Ryan Zimmerman", "email":"rzimmerman@talusanalytics.com", "organization":"Talus Analytics"}'\
  -X POST $api/create-user;

curl -d \
  '{"name":"David Rosado", "email":"drosado@talusanalytics.com", "organization":"Talus Analytics"}'\
  -X POST $api/create-user;

curl -d \
  '{"name":"Tess Stevens", "email":"tstevens@talusanalytics.com", "organization":"Talus Analytics"}'\
  -X POST $api/create-user;

curl -d \
  '{"name":"Ellie Graeden", "email":"egraeden@talusanalytics.com", "organization":"Talus Analytics"}'\
  -X POST $api/create-user;

curl -d \
  '{"name":"Hailey", "email":"hrobertson@talusanalytics.com", "organization":"Talus Analytics"}'\
  -X POST $api/create-user;

curl -d \
  '{"name":"Cassiana Robinson", "email":"crobinson@talusanalytics.com", "organization":"Talus Analytics"}'\
  -X POST $api/create-user;

curl -d \
  '{"name":"Colin Carlson", "email":"cjc322@georgetown.edu", "organization":"Talus Analytics"}'\
  -X POST $api/create-user;


