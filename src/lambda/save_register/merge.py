# we're not going to do types anymore

# class Datapoint(dict):
#     displayValue: str
#     dataValue: dict
#     version: str


# class Record(dict):
#     str: Datapoint


# class Register(dict):
#     str: Record


def merge(left, right):
    left_length = len(left)
    right_length = len(right)

    start = left
    target = right
    if right_length >= left_length:
        start = right
        target = left

    merged = {}

    for record_id in start:
        if record_id not in target:
            merged[record_id] = start[record_id]
            continue

        for datapoint_id in start[record_id]:
            if datapoint_id not in target[record_id][datapoint_id]:
                merged[record_id][datapoint_id] = start[record_id][datapoint_id]

    return merged
