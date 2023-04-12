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

