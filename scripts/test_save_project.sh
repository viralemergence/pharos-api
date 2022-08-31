
if [[ -z "$1" ]];
    then
        echo "Define an api"
        exit 1
    else
        api=$1
fi

curl $api/save-project \
  --data-raw '{"projectName":"test","description":"","projectType":"","surveillanceType":"","relatedMaterials":[""],"publicationsCiting":[""],"status":1,"projectID":"sdgjlgsgd0","authors":[{"researcherID":"dev","role":"owner"}],"datasets":{}}'

# "Succesful upload"%