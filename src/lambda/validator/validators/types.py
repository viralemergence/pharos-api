class Record:
    """
    Record object matching frontend definition.

    { [key: string]: Datapoint }

    """

    def __init__(self, record: dict):
        datapoints = {key: Datapoint(value) for key, value in record.items()}
        self.__dict__.update(datapoints)


class Datapoint:
    """
    Datapoint object matching frontend definition.

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
    """

    def __init__(self, datapoint: dict):
        self.__dict__.update(datapoint)
        datapoint_ = self
        if hasattr(datapoint_, "previous"):
            previous = Datapoint(datapoint_.previous)
            datapoint_.previous = previous


def unpack_datapoint(datapoint: Datapoint) -> set:
    unpacked_datapoints = set()
    while hasattr(datapoint, "previous"):
        unpacked_datapoints.add(datapoint)
        datapoint = datapoint.previous
    return unpacked_datapoints


def order_datapoints(datapoints: list) -> Datapoint:
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


def merge(client_record: Record, stored_record: Record) -> Record:
    # Client record
    client = {
        key: unpack_datapoint(Datapoint(value)) for key, value in client_record.items()
    }
    # Stored record
    stored = {
        key: unpack_datapoint(Datapoint(value)) for key, value in stored_record.items()
    }
    # Merge two dictionaries with common and shared keys
    merged_record = {
        k: client[k] + stored[k]
        if (k in client and k in stored)
        else client.get(k) or stored.get(k)
        for k in sorted(client.keys() | stored.keys())
    }
    # Nest datapoints
    merged_record = {k: order_datapoints(v) for k, v in merged_record.items()}
    # Create Record object
    merged_record = Record(merged_record)

    return merged_record
