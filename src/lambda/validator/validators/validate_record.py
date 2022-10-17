from .type import Datapoint, Record


def get_validator_class(name: str):
    module = getattr(__import__(f"validators.{name}"), name)
    return getattr(module, name.capitalize())


def validate_location(latitude: Datapoint, longitude: Datapoint) -> tuple:
    """Verify if the location is valid"""

    try:
        latitude_ = float(latitude.dataValue)
        longitude_ = float(longitude.dataValue)

        if -90 <= latitude_ <= 90 and -180 <= longitude_ <= 180:
            report = {"status": "Success"}

    except Exception:
        report = {"status": "Success", "message": "Invalid location."}

    latitude.report["_validate_location"] = report
    longitude.report["_validate_location"] = report

    return {"Latitude": latitude, "Location": longitude}


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
        key: Ncbi(key)
        for key in [
            "Host NCBI Tax ID",
            "Pathogen NCBI Tax ID",
            "Detection target NCBI Tax ID",
        ]
    }

    latin = {
        key: Latin(key) for key in ["Host species", "Pathogen", "Detection target"]
    }

    location = {key: Location(key) for key in ["Latitude", "Longitude"]}

    loc = validate_location(record.Latitude, record.Longitude)
    location.update(loc)

    record.__dict__.update({**ncbi, **latin, **location})
    return record
