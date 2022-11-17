import sys

sys.path.append("./src/lambda/save_register/")
from validator.definitions import Datapoint
from validator.latin import Latin

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

# Testing validation
assert not hasattr(datapoint, "report")
Latin(datapoint)
assert hasattr(datapoint, "report")
assert "status" in datapoint.report
assert datapoint.report["status"] == "SUCCESS"
