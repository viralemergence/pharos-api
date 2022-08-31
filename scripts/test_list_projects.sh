
if [[ -z "$1" ]];
    then
        echo "Define an api"
        exit 1
    else
        api=$1
fi

curl $api/list-projects /
    --data-raw '{"researcherID" : "dev"}'

# [{"projectID": "prjGDcNesfarg9d0", "surveillanceType": "", "status": "1", "relatedMaterials": [""], "datasets": {}, 
# "projectName": "test", "datasetIDs": [], "publicationsCiting": [""], "description": "", "projectType": "", 
# "authors": [{"researcherID": "dev", "role": "owner"}]}, {"projectID": "prjGDcNl5V9d0", "surveillanceType": "", "status": "1", 
# "datasets": {}, "relatedMaterials": [""], "projectName": "test", "datasetIDs": ["testing"], "publicationsCiting": [""], "description": "", 
# "projectType": "", "authors": [{"researcherID": "dev", "role": "owner"}]}, 
# {"projectID": "sdgjlgsgd0", "surveillanceType": "", "status": "1", "relatedMaterials": [""], "datasets": {}, "projectName": "test", 
# "publicationsCiting": [""], "description": "", "projectType": "", "authors": [{"researcherID": "dev", "role": "owner"}]}]%