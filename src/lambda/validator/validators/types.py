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


class MergeRecords:
    def __init__(self, client_record: Record, stored_record: Record) -> None:
        self.client_record = client_record
        self.stored_record = stored_record

    def merge(self):
        for key, value in self.client_record.items():
            if key in self.stored_record:
                self.stored_record[key].extend(value)
            else:
                self.stored_record[key] = value

        # TODO: order by timestamp

        return self.stored_record
