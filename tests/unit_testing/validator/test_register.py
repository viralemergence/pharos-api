import sys
import json

sys.path.append("./src/lambda/save_register/validator/")
from type import Datapoint, Record

with open("./tests/unit_testing/validator/register.json") as file:
    register = json.load(file)

register_ = {}

for key, value in register.items():
    record = Record(value, key)
    register_[record.id] = record.get_record()

assert register_ == register
