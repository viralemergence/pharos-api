import sys
import json
import timeit


sys.path.append("./src/lambda/save_register/")
from validator.definitions import Datapoint, Record
from validator.validate_record import validate_record

with open("./valid_test_register.json") as file:
    data = file.read()
    # register = json.load(file)


def old_validate_register(json_data: str):
    register = json.loads(json_data)

    verified_register = {}

    for record_id, record in register["register"].items():
        record_ = Record(record, record_id)
        record_ = validate_record(record_)
        verified_register[record_id] = record_.get_record()

    return verified_register


valid_register = old_validate_register(data)

register_time = timeit.timeit(lambda: old_validate_register(data), number=10)

print(register_time)

# print(valid_register["recH6U0vPQPHL"])


# assert register_ == register

# valid_register = {}

# for record_id, record in register.items():
#     record_ = Record(record, record_id)
#     valid_rec = validate_record(record_)

#     valid_register[record_id] = valid_rec.get_record()

# assert valid_register != register_
# assert valid_register != register

# for record in valid_register.values():
#     for k, dp in record.items():
#         assert "report" in dp
