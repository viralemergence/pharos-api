#!/bin/bash

api='http://localhost:3000'

# parse params
while [[ "$#" > 0 ]];
  do case $1 in
    -a|--api) api=$2; shift;;
    *) echo "Unknown parameter passed: $1"; exit 1 ;;
  esac
  shift
done


curl -d \
  '{"researcherID":"resdev", "name":"Ryan Zimmerman", "email":"rzimmerman@talusanalytics.com", "organization":"Talus Analytics"}'\
  -X POST $api/create-user;

# curl -d \
#   '{"researcherID":"f9fbebeba38b4628836e8c27f9ce9918", "name":"David Rosado", "email":"drosado@talusanalytics.com", "organization":"Talus Analytics"}'\
#   -X POST $api/create-user;

# curl -d \
#   '{"researcherID":"7ab11e694c7f44ff8e968575d809418e", "name":"Tess Stevens", "email":"tstevens@talusanalytics.com", "organization":"Talus Analytics"}'\
#   -X POST $api/create-user;

# curl -d \
#   '{"researcherID":"d012836ee96b4617809c81edc87f4219", "name":"Ellie Graeden", "email":"egraeden@talusanalytics.com", "organization":"Talus Analytics"}'\
#   -X POST $api/create-user;

# curl -d \
#   '{"researcherID":"a2d0efb40799454a8fbb254c627d2ac9", "name":"Hailey", "email":"hrobertson@talusanalytics.com", "organization":"Talus Analytics"}'\
#   -X POST $api/create-user;

# curl -d \
#   '{"researcherID":"ae2b13939b124d56977841df70cedb76", "name":"Colin Carlson", "email":"cjc322@georgetown.edu", "organization":"Talus Analytics"}'\
#   -X POST $api/create-user;


