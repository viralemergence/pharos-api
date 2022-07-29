#!/bin/bash

api=$PHAROS_API


while [[ "$#" > 0 ]];
  do case $1 in
    -a|--api) api=$2; shift;;
    *) echo "Unknown parameter passed: $1"; exit 1 ;;
  esac
  shift
done


curl -d \
  '{"researcherID" : "12345", "dataset_name" : "zoo", "samples_taken" : 1, "detection_run" : 0}'\
  -X POST $api/create-dataset;
