from devtools import debug
from register_model import Register

example_data = """
{
    "rec9Hjw3utQm2": {
        "Host species NCBI tax ID": {
            "displayValue": "3053800000",
            "dataValue": "30538000000",
            "modifiedBy": "dev",
            "version": "1678391083301"
        },
        "Sample ID": {
            "displayValue": "1",
            "dataValue": "1",
            "report": {
                "status": "SUCCESS",
                "message": "Ready to release."
            },
            "modifiedBy": "dev",
            "version": "1678461815020"
        },
        "Host species": {
            "displayValue": "Bat",
            "dataValue": "Bat",
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
        }
    }
}
"""

register = Register.parse_raw(f'{{"data":{example_data}}}')

debug(register)
