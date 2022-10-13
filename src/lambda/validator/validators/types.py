class Record:
    def __init__(self, record: dict):
        datapoints = {key: Datapoint(value) for key, value in record.items()}
        self.__dict__.update(datapoints)


class Datapoint:
    def __init__(self, datapoint: dict):
        self.__dict__.update(datapoint)
        datapoint_ = self
        if hasattr(datapoint_, "previous"):
            previous = Datapoint(datapoint_.previous)
            datapoint_.previous = previous


def order_datapoints(datapoints : list) -> Datapoint:
    # Sort list from oldest to latest timestamp
    sorted(datapoints, key=lambda datapoint : datapoint.timestamp, reverse=True)
    # Extract the latest datapoint
    oldest = datapoints.pop(0)
    # Nest datapoints
    while datapoints:
        latest = datapoints.pop(0)
        latest.preview = oldest
        oldest = latest
    return oldest

def merge(client_record: Record, stored_record: Record):
    for key, value in client_record.items():
        if key in stored_record:
            stored_record[key].extend(value)
        else:
            stored_record[key] = value

    datapoints = {}
    for column, datapoint in 
    

    return stored_record
