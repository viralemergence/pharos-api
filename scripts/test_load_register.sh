
if [[ -z "$1" ]];
    then
        echo "Define an api"
        exit 1
    else
        api=$1
fi

curl $api/load-register \
    --data-raw '{"researcherID" : "dev", "datasetID" : "testing"}'


# [{"datasetID": "testing", "recordID": "_meta", "record": {"something": "some something"}}, 
# {"datasetID": "testing", "recordID": "rec4gCNJK390w", "record": {"Animal nickname": {"displayValue": "cat", "dataValue": "cat", "modifiedBy": "dev", "version": "1"}, "SampleID": {"displayValue": "ref3", "dataValue": "ref3", "modifiedBy": "dev", "version": "0"}, "Animal ID": {"displayValue": "bla3", "dataValue": "bla3", "modifiedBy": "dev", "version": "1"}}}, 
# {"datasetID": "testing", "recordID": "recLfH3svdhwW", "record": {"Animal nickname": {"displayValue": "fat", "dataValue": "fat", "modifiedBy": "dev", "version": "1"}, "SampleID": {"displayValue": "ref2", "dataValue": "ref2", "modifiedBy": "dev", "version": "0"}, "Animal ID": {"displayValue": "bla2", "dataValue": "bla2", "modifiedBy": "dev", "version": "1"}}}, 
# {"datasetID": "testing", "recordID": "recfx92jKumL0", "record": {"Animal nickname": {"displayValue": "rat", "dataValue": "rat", "modifiedBy": "dev", "version": "1"}, "SampleID": {"displayValue": "ref4", "dataValue": "ref4", "modifiedBy": "dev", "version": "0"}, "Animal ID": {"displayValue": "bla4", "dataValue": "bla4", "modifiedBy": "dev", "version": "1"}}}, 
# {"datasetID": "testing", "recordID": "recvJdnNnAV15", "record": {
#     "Animal ID": {"displayValue": "bla1", "dataValue": "bla1", "modifiedBy": "dev", "version": "0"}, 
#     "Collection Method or Tissue": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}, 
#     "Detection Comments": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}, 
#     "Host": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}, 
#     "Collection Date": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"},
#     "Latitude": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"},
#     "Detection target": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}, 
#     "Longitude": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"},
#     "Detection Outcome": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}, 
#     "Detection Method": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}, 
#     "Pathogen NCBI Tax ID": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}, 
#     "GenBank Accession": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}, 
#     "Animal nickname": {"displayValue": "brat", "dataValue": "brat", "modifiedBy": "dev", 
#     "previous": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}, "version": "1"}, 
#     "Pathogen": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}, 
#     "Target CBCI Tax ID": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}, 
#     "SampleID": {"displayValue": "ref1", "dataValue": "ref1", "modifiedBy": "dev", "version": "0"}, 
#     "Spatial uncertainty": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "0"}}}]%