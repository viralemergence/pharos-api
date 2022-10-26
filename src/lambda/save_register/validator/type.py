class Datapoint:
    """
    Datapoint object matching frontend definition.
    {
    displayValue: string
    dataValue: string | { [key: string]: string }
    report?: {
        pass: ReportScore
        message: string
        data: { [key: string]: string }
    }
    modifiedBy: string
    version: string
    previous?: Datapoint
    }
    """

    # Create an immutable object. Only allows the creation of valid datapoints per definition.
    # Don't allow inconsistent objects to be created in the first place.
    # Using slots reduces memory since there is no dynamic allocation and no weak references.

    __slots__ = (
        "displayValue",
        "dataValue",
        "report",
        "modifiedBy",
        "version",
        "timestamp",
        "previous",
    )

    def __init__(self, datapoint: dict):
        for k, v in datapoint.items():
            setattr(self, k, v)

        # Recursively create datapoints through linked list.
        if hasattr(self, "previous"):
            previous = Datapoint(self.previous)
            self.previous = previous

    def get_datapoint(self) -> dict:
        dictionary = {s: getattr(self, s) for s in self.__slots__ if hasattr(self, s)}
        if hasattr(self, "previous"):
            previous = dictionary["previous"]
            dictionary["previous"] = previous.get_datapoint()
        return dictionary


class Record:
    """
    Record object matching frontend definition.

    { [key: string]: Datapoint }

    """

    def __init__(self, record: dict):
        datapoints = {key: Datapoint(value) for key, value in record.items()}
        self.__dict__.update(datapoints)

    def get_record(self) -> dict:
        return {key: value.get_datapoint() for key, value in self.__dict__.items()}
