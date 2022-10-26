import sys

sys.path.append("./src/lambda/save_register/validator/")
from type import Datapoint, Record
from latin import Latin

# Datapoint
host_species = {
    "displayValue": "Vulpes vulpes",
    "dataValue": "Vulpes vulpes",
    "modifiedBy": "dev",
    "version": "2",
    "previous": {
        "displayValue": "Vulpes vulpes",
        "dataValue": "Vulpes vulpes",
        "modifiedBy": "dev",
        "version": "1",
    },
}

# Testing datapoint equality
datapoint = Datapoint(host_species)
assert datapoint.get_datapoint() == host_species
