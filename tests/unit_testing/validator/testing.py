import json
import os

import sys

sys.path.append("./src/lambda/save_register/validator/")
from type import Datapoint, Record


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

datapoint = Datapoint(host_species)
datapoint.get_datapoint()

assert datapoint.get_datapoint() == host_species
