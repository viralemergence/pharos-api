from .definitions import Datapoint, Record
from .ncbi import Ncbi
from .latin import Latin
from .location import Location
from .default import Default
from .date import Date


def validate_record(record: Record) -> Record:

    default = {
        key: Default(getattr(record, key)).datapoint
        for key in [
            "Row ID",
            "Sample ID",
            "Animal ID",
            "Spatial uncertainty",
            "Spatial uncertainty units",
            "Collection method or tissue",
            "Detection method",
            "Primer sequence",
            "Primer citation",
            "Detection outcome",
            "Detection measurement",
            "Detection measurement units",
            "GenBank accession",
            "Detection comments",
            "Organism sex",
            "Dead or alive",
            "Health notes",
            "Life stage",
            "Age",
            "Age units",
            "Mass",
            "Mass units",
            "Length",
            "Length units",
        ]
        if hasattr(record, key)
    }

    ncbi = {
        key: Ncbi(getattr(record, key)).datapoint
        for key in [
            "Host NCBI tax ID",
            "Pathogen NCBI tax ID",
            "Detection target NCBI tax ID",
        ]
        if hasattr(record, key)
    }

    latin = {
        key: Latin(getattr(record, key)).datapoint
        for key in ["Host species", "Pathogen", "Detection target"]
        if hasattr(record, key)
    }

    location = {
        key: Location(getattr(record, key), record).datapoint
        for key in ["Latitude", "Longitude"]
        if hasattr(record, key)
    }

    date = {
        key: Date(getattr(record, key), record).datapoint
        for key in [
            "Collection day",
            "Collection month",
            "Collection year",
        ]
        if hasattr(record, key)
    }

    keys = record.__dict__.keys()
    valid_keys = (
        list(ncbi.keys())
        + list(latin.keys())
        + list(location.keys())
        + list(default.keys())
        + list(date.keys())
        + ["id"]
    )
    keys_ = [key for key in keys if key not in valid_keys]

    non_recognized = {"status": "WARNING", "message": "Datapoint is not recognized."}

    for k in keys_:
        value = record.__dict__[k]
        setattr(value, "report", non_recognized)
        record.__dict__[k] = value

    record.__dict__.update({**ncbi, **latin, **location, **default, **date})
    return record
