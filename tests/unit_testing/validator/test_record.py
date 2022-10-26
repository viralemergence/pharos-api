import sys

sys.path.append("./src/lambda/save_register/validator/")
from type import Record
from validate_record import validate_record


# Testing record
recDyDAzCf8v5 = {
    "Row ID": {
        "displayValue": "1",
        "dataValue": "1",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Test ID": {
        "displayValue": "1",
        "dataValue": "1",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Sample ID": {
        "displayValue": "1",
        "dataValue": "1",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Animal ID or nickname": {
        "displayValue": "1",
        "dataValue": "1",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Host species": {
        "displayValue": "Vulpes vulpes",
        "dataValue": "Vulpes vulpes",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Host NCBI Tax ID": {
        "displayValue": "9627",
        "dataValue": "9627",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Latitude": {
        "displayValue": "52.547625",
        "dataValue": "52.547625",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Longitude": {
        "displayValue": "13.396958",
        "dataValue": "13.396958",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Spatial uncertainty": {
        "displayValue": "",
        "dataValue": "",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Collection date": {
        "displayValue": "21/06/2008",
        "dataValue": "21/06/2008",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Collection method or tissue": {
        "displayValue": "spleen, bowel, lung, brain, lymph node",
        "dataValue": "spleen, bowel, lung, brain, lymph node",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Detection method": {
        "displayValue": "Direct immunofluorescence assay",
        "dataValue": "Direct immunofluorescence assay",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Detection target": {
        "displayValue": "Canine morbillivirus",
        "dataValue": "Canine morbillivirus",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Detection target NCBI Tax ID": {
        "displayValue": "11232",
        "dataValue": "11232",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Detection measurement": {
        "displayValue": "",
        "dataValue": "",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Detection outcome": {
        "displayValue": "Negative",
        "dataValue": "Negative",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Pathogen": {
        "displayValue": "Canine morbillivirus",
        "dataValue": "Canine morbillivirus",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Pathogen NCBI Tax ID": {
        "displayValue": "11232",
        "dataValue": "11232",
        "modifiedBy": "dev",
        "version": "1",
    },
    "GenBank Accession": {
        "displayValue": "",
        "dataValue": "",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Detection comments": {
        "displayValue": "",
        "dataValue": "",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Organism sex": {
        "displayValue": "M",
        "dataValue": "M",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Dead or alive": {
        "displayValue": "Dead",
        "dataValue": "Dead",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Health notes": {
        "displayValue": "",
        "dataValue": "",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Life stage": {
        "displayValue": "",
        "dataValue": "",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Age": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "1"},
    "Age units": {
        "displayValue": "",
        "dataValue": "",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Mass": {"displayValue": "", "dataValue": "", "modifiedBy": "dev", "version": "1"},
    "Mass units": {
        "displayValue": "",
        "dataValue": "",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Length": {
        "displayValue": "",
        "dataValue": "",
        "modifiedBy": "dev",
        "version": "1",
    },
    "Length units": {
        "displayValue": "",
        "dataValue": "",
        "modifiedBy": "dev",
        "version": "1",
    },
}

# Test record equality
record = Record(recDyDAzCf8v5, "recDyDAzCf8v5")
assert record.get_record() == recDyDAzCf8v5

# Test record validation
record_ = validate_record(record)

valid_keys = [
    "Host NCBI Tax ID",
    "Pathogen NCBI Tax ID",
    "Detection target NCBI Tax ID",
    "Host species",
    "Pathogen",
    "Detection target",
    "Latitude",
    "Longitude",
    "id",
]

for key, value in record_.__dict__.items():
    if key not in valid_keys:
        datapoint = record_.__dict__[key].get_datapoint()

        assert datapoint["report"]["status"] == "WARNING"
        assert datapoint["report"]["message"] == "Datapoint is not recognized."
