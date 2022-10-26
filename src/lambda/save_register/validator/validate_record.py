from type import Datapoint, Record


def get_validator_class(name: str):
    module = getattr(__import__(name), name.capitalize())
    return module


def validate_location(latitude: Datapoint, longitude: Datapoint) -> dict:
    """Verify if the location is valid"""

    try:
        latitude_ = float(latitude.dataValue)
        longitude_ = float(longitude.dataValue)

        if -90 <= latitude_ <= 90 and -180 <= longitude_ <= 180:
            report = {"status": "Success"}

    except Exception:
        report = {"status": "Failure", "message": "Invalid location."}

    latitude.report["_validate_location"] = report
    longitude.report["_validate_location"] = report

    return {"Latitude": latitude, "Location": longitude}  # Maybe unnecesary return


def validate_date(year, month, day) -> tuple:
    try:
        report = {"status": "Success"}
    except Exception:
        report = {"status": "Success", "message": "Invalid date."}

    return


def validate_record(record: Record) -> Record:

    # Retrieve validator
    Ncbi, Latin, Location = map(get_validator_class, ["ncbi", "latin", "location"])

    ncbi = {
        key: Ncbi(getattr(record, key)).datapoint
        for key in [
            "Host NCBI Tax ID",
            "Pathogen NCBI Tax ID",
            "Detection target NCBI Tax ID",
        ]
        if hasattr(record, key)
    }

    latin = {
        key: Latin(getattr(record, key)).datapoint
        for key in ["Host species", "Pathogen", "Detection target"]
        if hasattr(record, key)
    }

    location = {
        key: Location(getattr(record, key)).datapoint
        for key in ["Latitude", "Longitude"]
        if hasattr(record, key)
    }

    keys = record.__dict__.keys()
    valid_keys = list(ncbi.keys()) + list(latin.keys()) + list(location.keys()) + ["id"]
    keys_ = [key for key in keys if key not in valid_keys]

    non_recognized = {"status": "WARNING", "message": "Datapoint is not recognized."}

    for k in keys_:
        value = record.__dict__[k]
        setattr(value, "report", non_recognized)
        record.__dict__[k] = value

    record.__dict__.update({**ncbi, **latin, **location})
    return record
