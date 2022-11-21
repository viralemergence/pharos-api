import datetime
from models import Records, ResearchersRecords


def preprocess_record(record, record_id, dataset_id, project_id) -> dict:
    # Keys to lowercase and unpack datavalue
    record_ = {k.lower().replace(" ", "_"): v["dataValue"] for k, v in record.items()}
    # Create geometry point
    record_.update({"location": f"POINT({record_['longitude']} {record_['latitude']})"})
    # Create date
    record_.update(
        {
            "collection_date": datetime.datetime.strptime(
                record_["collection_date"], "%d/%m/%Y"
            ).date()
        }
    )
    # Add pharos id
    record_.update(
        {
            "record_id": record_id,
            "dataset_id": dataset_id,
            "project_id": project_id,
            "pharos_id": f"{project_id}-{dataset_id}-{record_id}",
        }
    )

    return record_


def create_records(pharosId, researcherId, record):

    research_record = ResearchersRecords(researcher_id=researcherId, record_id=pharosId)

    pharos_record = Records(**preprocess_record(record))

    return pharos_record, research_record
