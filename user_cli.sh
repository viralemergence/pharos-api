#!/bin/bash

while test $# -gt 0; do
    case "$1" in
        -a|--api)
            shift
            if test $# -gt 0;
                then export API=$1
            else
                echo "API endpoint is missing"
                exit 1
            fi
            shift
            ;;
        -n|--name)
            shift
            if test $# -gt 0;
                then export NAME=$1
            else
                echo "Name was not specified"
                exit 1;
            fi
            shift
            ;;
        -e|--email)
            shift
            if test $# -gt 0; 
                then export EMAIL=$1
            else
                echo "Email was not specified"
                exit 1;
            fi
            shift
            ;;
        -o|--organization)
            shift
            if test $# -gt 0;
                then export ORGANIZATION=$1
            else
                echo "Organization was not specified"
                exit 1
            fi
            shift
            ;;
        -i|--id)
            shift
            if test $# -gt 0;
                then export ID=$1
            else
                echo "researcherID was not specified"
                exit 1
            fi
            shift
            ;;
        *)
            break
            ;;
    esac
done

if [[ $ID ]];
    then curl $API/create-user -d '{"name" : "'"$NAME"'", "email" : "'"$EMAIL"'", "organization" : "'"$ORGANIZATION"'", "researcherID" : "'"$ID"'"}'
else
    curl $API/create-user -d '{"name" : "'"$NAME"'", "email" : "'"$EMAIL"'", "organization" : "'"$ORGANIZATION"'"}'
fi


