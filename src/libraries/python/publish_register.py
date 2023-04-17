from datetime import date
from typing import Union
from devtools import debug
from sqlalchemy.orm import Session
from models2 import PublishedRecord
from register import Datapoint, Dataset, Project, Register


def publish_register_to_session(
    register: Register, dataset: Dataset, project: Project, session: Session
):
    """Publish a register to the database"""

    id_prefix = project.projectID + "-" + dataset.datasetID
    for recordID, record in register.register_data.__dict__.items():
        pharos_id = id_prefix + "-" + recordID

        record_dict: dict[str, Datapoint] = record.__dict__

        exclude = {
            "collection_day",
            "collection_month",
            "collection_year",
            "latitude",
            "longitude",
        }

        prepublish: dict[str, Union[Datapoint, date]] = {}

        for field, datapoint in record_dict.items():
            if datapoint is not None and field not in exclude:
                prepublish[field] = datapoint

        debug(prepublish)

        if (
            not record.collection_day
            or not record.collection_month
            or not record.collection_year
        ):
            raise ValueError(
                "Record is missing collection date, should not have passed validator"
            )

        collection_date = date(
            int(record.collection_year),
            int(record.collection_month),
            int(record.collection_day),
        )

        if not record.latitude or not record.longitude:
            raise ValueError(
                "Record is missing location, should not have passed validator"
            )

        location = f"POINT({record.latitude},{record.longitude})"

        published = PublishedRecord(
            pharos_id=pharos_id,
            attributions=attributions,
            collection_date=collection_date,
            location=location,
            **prepublish,
        )

        session.add(published)
