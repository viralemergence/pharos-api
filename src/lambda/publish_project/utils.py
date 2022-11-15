from models import Records, ResearchersRecords


def create_records(pharosId, researcherId, record):

    pr, dt, rec = pharosId.split("-")

    research_record = ResearchersRecords(researcher_id=researcherId, record_id=pharosId)

    record_ = Records(
        pharos_id=pharosId,
        project_id=pr,
        dataset_id=dt,
        record_id=rec,
        test_id=record["Test ID"]["dataValue"],
        sample_id=record["Sample ID"]["dataValue"],
        # pool=1,  # record["Pool"]["dataValue"],
        animal_id=record["Animal ID or nickname"]["dataValue"],
        collection_method=record["Collection method or tissue"]["dataValue"],
        detection_method=record["Detection method"]["dataValue"],
        detection_target=record["Detection target"]["dataValue"],
        detection_target_ncbi_tax_id=record["Detection target NCBI Tax ID"][
            "dataValue"
        ],
        pathogen=record["Pathogen"]["dataValue"],
        pathogen_ncbi_tax_id=record["Pathogen NCBI Tax ID"]["dataValue"],
        detection_outcome=record["Detection outcome"]["dataValue"],
        location=f"POINT({record['Longitude']['dataValue']} {record['Latitude']['dataValue']})",
        spatial_uncertainity=record["Spatial uncertainity"]["dataValue"],
        collection_date=datetime.datetime.strptime(
            record["Collection date"]["dataValue"], "%d/%m/%Y"
        ).date(),  # datetime.datetime( f"{record["Year"]}/{record["Month"]}/{record["Day"]}", '%y/%m/%d')
        host_species=record["Host species"]["dataValue"],
        host_ncbi_tax_id=record["Host NCBI Tax ID"]["dataValue"],
        # host_uncertanity="",  # record["Uncertaninity"]["dataValue"],
        life_stage=record["Life stage"]["dataValue"],
        organism_sex=record["Organism sex"]["dataValue"],
        dead_or_alive=record["Dead or alive"]["dataValue"],
        age=float(record["Age"]["dataValue"]),
        mass=float(record["Mass"]["dataValue"]),
        length=float(record["Length"]["dataValue"]),
    )

    return record_, research_record
