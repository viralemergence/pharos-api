from type import Datapoint


def get_validator_class(name):
    try:
        module = getattr(__import__(f"validators.{name}"), name)
        return getattr(module, name)
    except Exception:
        return getattr("")


def validate_record(record: "Record"):

    for datapointID, datapoint in record.__dict__:
        # Retrieve the appropriate validator class
        Validator = get_validator_class(datapointID)
        # Dynamically create a new class: Datapoint + Validator.
        # Inherits __init__ from Datapoint and all methods and attributes from both classes
        DatapointValidator = type("DatapointValidator", (Datapoint, Validator))
        # Instantiate an object of the newly defined class
        valid_datapoint = DatapointValidator(datapoint)
        # Run validation over datapoint
        valid_datapoint.run_validation()
        # Update record
        record.datapointID = valid_datapoint

    return record
