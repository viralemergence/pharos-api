# pylint: skip-file
# pylint: disable - all

# import datetime
# from models import Tests, ResearchersTests


# def verify_record(record: dict) -> bool:

#     for datapoint in record.values():
#         if datapoint["report"]["status"] == "FAIL":
#             return False

#     return True


# def preprocess_record(record, record_id, dataset_id, project_id) -> dict:
#     # Keys to lowercase and unpack datavalue
#     record_ = {k.lower().replace(" ", "_"): v["dataValue"] for k, v in record.items()}
#     # Create geometry point
#     record_.update({"location": f"POINT({record_['longitude']} {record_['latitude']})"})
#     # Create date
#     record_.update(
#         {
#             "collection_date": datetime.datetime.strptime(
#                 f"{record_['collection_day']}/{record_['collection_month']}/{record_['collection_year']}",
#                 "%d/%m/%Y",
#             ).date()
#         }
#     )
#     # Add pharos id
#     record_.update(
#         {
#             "record_id": record_id,
#             "dataset_id": dataset_id,
#             "project_id": project_id,
#             "pharos_id": f"{project_id}-{dataset_id}-{record_id}",
#         }
#     )

#     return record_


# def create_records(project_id, dataset_id, record_id, researcher_id, record):

#     researcher_test_record = ResearchersTests(
#         researcher_id=researcher_id, record_id=f"{project_id}-{dataset_id}-{record_id}"
#     )

#     test_record = Tests(**preprocess_record(record, record_id, dataset_id, project_id))

#     return test_record, researcher_test_record
