from typing import Dict

from devtools import debug
from pydantic import BaseModel, Field, ValidationError
from register import Record


example_data = """
{
    "datasetID": "testing-dataset",
    "researcherID": "dev",
    "register": {
        "rec9Hjw3utQm2": {
            "Host species NCBI tax ID": {
                "displayValue": "",
                "dataValue": "",
                "report": {
                    "status": "WARNING",
                    "message": "Datapoint is not recognized."
                },
                "modifiedBy": "dev",
                "version": "1678391083301"
            },
            "Sample ID": {
                "displayValue": "1",
                "dataValue": "1",
                "modifiedBy": "dev",
                "version": "1678461815020"
            },
            "Host species": {
                "displayValue": "Bat",
                "dataValue": "Bat",
                "report": {
                    "status": "SUCCESS",
                    "message": "Ready to release."
                },
                "modifiedBy": "dev",
                "version": "1678461820035"
            },
            "Animal ID": {
                "displayValue": "Fred",
                "dataValue": "Fred",
                "report": {
                    "status": "SUCCESS",
                    "message": "Ready to release."
                },
                "modifiedBy": "dev",
                "version": "1678461824903",
                "previous": {
                    "displayValue": "Old Name",
                    "dataValue": "Old name",
                    "modifiedBy": "dev",
                    "version": "1678461824903"
                }
            },
            "Latitude": {
                "displayValue": "pear",
                "dataValue": "12.45",
                "modifiedBy": "dev",
                "version": "1678461815020"
            },
            "Lotitude": {
                "displayValue": "pear",
                "dataValue": "12.45",
                "modifiedBy": "dev",
                "version": "1678461815020"
            },
            "Collection day": {
                "dataValue": "22",
                "modifiedBy": "dev",
                "version": "1678461815020"
            },
            "Collection month": {
                "dataValue": "12",
                "modifiedBy": "dev",
                "version": "1678461815020"
            },
            "Collection year": {
                "dataValue": "2022",
                "modifiedBy": "dev",
                "version": "1678461815020"
            }
        }
    }
}
"""


# with open("./valid_test_register.json", "r", encoding="utf-8") as f:
#     example_data = f.read()


class SaveRegisterData(BaseModel):
    researcherID: str
    datasetID: str
    register_data: Dict[str, Record] = Field(..., alias="register")


# data = SaveRegisterData.parse_raw(example_data)


def lambda_handler(event):
    try:
        data = SaveRegisterData.parse_raw(event)
    except ValidationError as e:
        print(e.json(indent=2))
        return {"statusCode": 400, "body": e.json()}

    print(data.json(exclude_none=True, indent=2))

    # serialized = data.json(
    #     include={"register_data"}, by_alias=True, exclude_none=True
    # ).encode("utf-8")
    # print(serialized)

    return {"statusCode": 200, "body": "All good!"}


if __name__ == "__main__":
    debug(lambda_handler(example_data))


# def test_register(data_string: str):
#     raw = json.loads(data_string)
#     return Register.parse_obj({"data": raw["register"]})


# register_time = timeit.timeit(lambda: test_register(example_data), number=10)

# register2_time = timeit.timeit(lambda: Register.parse(example_data), number=1000)

# print(register_time)

# register = test_register(example_data)

# with open("./json_out.json", "w", encoding="utf-8") as f:
#     f.write(register.json(by_alias=True, exclude_none=True))


# with open("./pickle_out.pickle", "wb") as f:
#     pickle.dump(register, f)

# record = register.data["rec8lqwG85TMr"]

# debug(record.Animal_ID)
# debug(record.Animal_ID.isnumeric())
# debug(record.Animal_ID)

# print(register.data["rec8lqwG85TMr"].json(indent=2, by_alias=True, exclude_none=True))
