"""The mapping from pythonic snake case names
to capitalized, space-separated names to display
in the user interface. Unrecognized names will
be unchanged.
"""


API_NAME_TO_UI_NAME_MAP = {
    "sample_id": "Sample ID",
    "animal_id": "Animal ID",
    "host_species": "Host species",
    "host_species_ncbi_tax_id": "Host species NCBI tax ID",
    "collection_day": "Collection day",
    "collection_month": "Collection month",
    "collection_year": "Collection year",
    "collection_date": "Collection date",
    "collection_method_or_tissue": "Collection method or tissue",
    "detection_method": "Detection method",
    "primer_sequence": "Primer sequence",
    "primer_citation": "Primer citation",
    "detection_target": "Detection target",
    "detection_target_ncbi_tax_id": "Detection target NCBI tax ID",
    "detection_outcome": "Detection outcome",
    "detection_measurement": "Detection measurement",
    "detection_measurement_units": "Detection measurement units",
    "pathogen": "Pathogen",
    "pathogen_ncbi_tax_id": "Pathogen NCBI tax ID",
    "genbank_accession": "GenBank accession",
    "detection_comments": "Detection comments",
    "organism_sex": "Organism sex",
    "dead_or_alive": "Dead or alive",
    "health_notes": "Health notes",
    "life_stage": "Life stage",
    "age": "Age",
    "mass": "Mass",
    "length": "Length",
    "latitude": "Latitude",
    "longitude": "Longitude",
    "spatial_uncertainty": "Spatial uncertainty",
    "project_name": "Project",
    "researcher_name": "Researcher",
}

UI_NAME_TO_API_NAME_MAP = {v: k for k, v in API_NAME_TO_UI_NAME_MAP.items()}


def get_ui_name(api_name: str) -> str:
    """Convert from a snake_case pythonic column name
    to the capitalized, space-separated name to display
    in the user interface."""
    return API_NAME_TO_UI_NAME_MAP.get(api_name, api_name)


def get_api_name(ui_name: str) -> str:
    """Convert from a capitalized, space-separated name
    to the snake_case pythonic column name."""
    return UI_NAME_TO_API_NAME_MAP.get(ui_name, ui_name)
