#!/bin/bash

if [[ -z "$1" ]];
    then
        exit 1
    else
        API=$1
fi

curl -X POST -d "@payload_publish_register.json" $API/publish-project