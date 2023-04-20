from datetime import date
from geoalchemy2 import WKTElement
from sqlalchemy.orm import Session
from models import PublishedRecord, Researcher
from register import Datapoint, Register


def publish_register_to_session(
    register: Register,
    project_id: str,
    dataset_id: str,
    researchers: list[Researcher],
    session: Session,
):
    """Publish a register to the database session"""

    for record_id, record in register.register_data.items():

        published_record = PublishedRecord(
            pharos_id=project_id + "-" + dataset_id + "-" + record_id,
            project_id=project_id,
            dataset_id=dataset_id,
            record_id=record_id,
        )

        # Add all simple fields where one datapoint maps to
        # one database column and translation is unnecessary
        skip = {
            # date component fields
            "collection_day",
            "collection_month",
            "collection_year",
            # location component fields
            "latitude",
            "longitude",
        }
        record_dict: dict[str, Datapoint] = record.__dict__
        for field, datapoint in record_dict.items():
            if datapoint is not None and field not in skip:
                setattr(published_record, field, datapoint)

        # create and add the collection_date object
        if (
            not record.collection_day
            or not record.collection_month
            or not record.collection_year
        ):
            raise ValueError(
                "Record is missing collection date, should not have passed validator"
            )

        published_record.collection_date = date(
            int(record.collection_year),
            int(record.collection_month),
            int(record.collection_day),
        )

        # create and add the location geometry object
        if not record.latitude or not record.longitude:
            raise ValueError(
                "Record is missing location, should not have passed validator"
            )

        published_record.location = WKTElement(
            f"POINT({record.longitude} {record.latitude})"
        )

        published_record.researchers.extend(researchers)

        session.add(published_record)
