
if [[ -z "$1" ]];
    then
        echo "Define an api"
        exit 1
    else
        api=$1
fi

curl $api/save-register \
    --data-raw '{"researcherID" : "dev", "datasetID" : "testing", "register": {"recvJdnNnAV15": {"SampleID": {"displayValue": "ref1", "dataValue": "ref1", "version": "0", "modifiedBy": "dev"}, 
                            "Animal ID": {"displayValue": "bla1", "dataValue": "bla1", "version": "0", "modifiedBy": "dev"}, 
                            "Animal nickname": {"displayValue": "brat", "dataValue": "brat", "version": "1", "modifiedBy": "dev", "previous": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}}, 
                            "Host": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Collection Date": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Latitude": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Longitude": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Spatial uncertainty": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Collection Method or Tissue": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Detection Method": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Detection Outcome": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Detection target": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Target CBCI Tax ID": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Pathogen": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Pathogen NCBI Tax ID": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "GenBank Accession": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}, 
                            "Detection Comments": {"displayValue": "", "dataValue": "", "version": "0", "modifiedBy": "dev"}}, 
            "recLfH3svdhwW": {"SampleID": {"displayValue": "ref2", "dataValue": "ref2", "modifiedBy": "dev", "version": "0"}, 
                            "Animal ID": {"displayValue": "bla2", "dataValue": "bla2", "modifiedBy": "dev", "version": "1"}, 
                            "Animal nickname": {"displayValue": "fat", "dataValue": "fat", "modifiedBy": "dev", "version": "1"}}, 
            "rec4gCNJK390w": {"SampleID": {"displayValue": "ref3", "dataValue": "ref3", "modifiedBy": "dev", "version": "0"}, 
                            "Animal ID": {"displayValue": "bla3", "dataValue": "bla3", "modifiedBy": "dev", "version": "1"}, 
                            "Animal nickname": {"displayValue": "cat", "dataValue": "cat", "modifiedBy": "dev", "version": "1"}}, 
            "recfx92jKumL0": {"SampleID": {"displayValue": "ref4", "dataValue": "ref4", "modifiedBy": "dev", "version": "0"}, 
                            "Animal ID": {"displayValue": "bla4", "dataValue": "bla4", "modifiedBy": "dev", "version": "1"}, 
                            "Animal nickname": {"displayValue": "rat", "dataValue": "rat", "modifiedBy": "dev", "version": "1"}}}, 
        "versions": [{"date": "Thu, 04 Aug 2022 15:22:03 GMT", "name": "Thu, 04 Aug 2022 15:22:03 GMT"}, {"date": "Thu, 04 Aug 2022 15:22:54 GMT", "name": "Thu, 04 Aug 2022 15:22:54 GMT"}]}'


# "Succesful upload"%