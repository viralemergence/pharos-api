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

    def __hash__(self) -> int:
        return hash(self.timestamp)

    def __eq__(self, __o) -> bool:
        return (
            isinstance(__o, Datapoint) and self.get_datapoint() == __o.get_datapoint()
        )

    def get_datapoint(self) -> dict:
        return {s: getattr(self, s) for s in self.__slots__ if hasattr(self, s)}


class Record:
    """
    Record object matching frontend definition.

    { [key: string]: Datapoint }

    """

    def __init__(self, record: dict):
        datapoints = {key: Datapoint(value) for key, value in record.items()}
        self.__dict__.update(datapoints)

    @staticmethod
    def __unpack_datapoint(datapoint: Datapoint) -> set:
        unpacked_datapoints = set()
        while hasattr(datapoint, "previous"):
            unpacked_datapoints.add(datapoint)
            datapoint = datapoint.previous
        return unpacked_datapoints

    @staticmethod
    def __order_datapoints(datapoints: list) -> Datapoint:
        # Sort list from oldest to latest timestamp
        sorted(datapoints, key=lambda datapoint: datapoint.timestamp, reverse=True)
        # Extract the latest datapoint
        oldest = datapoints.pop(0)
        # Nest datapoints
        while datapoints:
            latest = datapoints.pop(0)
            latest.preview = oldest
            oldest = latest
        return oldest

    def __iadd__(self, __record):
        """
        Updates the record by overloading the operator +=.
        This can be achieved with: record1 += record2. Note that only record1 is modified.
        """
        # # Client record
        client = {
            key: self.__unpack_datapoint(value) for key, value in self.__dict__.items()
        }
        # Stored record
        stored = {
            key: self.__unpack_datapoint(value)
            for key, value in __record.__dict__.items()
        }
        # Merge two dictionaries with common and shared keys
        merged_record = {
            k: client[k].union(stored[k])
            if (k in client and k in stored)
            else client.get(k) or stored.get(k)
            for k in sorted(client.keys() | stored.keys())
        }
        # Nest datapoints and update the record
        self.__dict__.update(
            {k: self.__order_datapoints(list(v)) for k, v in merged_record.items()}
        )
