import sys
import json


sys.path.append("./src/lambda/save_register/validator/")
from type import Datapoint, Record
from validate_record import validate_record

with open("./tests/unit_testing/validator/register.json") as file:
    register = json.load(file)

register_ = {}

for key, value in register.items():
    record = Record(value, key)
    register_[record.id] = record.get_record()

assert register_ == register

valid_register = {}

for record_id, record in register.items():
    record_ = Record(record, record_id)
    valid_rec = validate_record(record_)

    valid_register[record_id] = valid_rec.get_record()

assert valid_register != register_
assert valid_register != register

for record in valid_register.values():
    for k, dp in record.items():
        assert "report" in dp
