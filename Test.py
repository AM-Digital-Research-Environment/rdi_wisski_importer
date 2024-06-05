from functions import mongodata_fetch

data = mongodata_fetch("projects_metadata_ubt","Humanitar2019")

data_1 = data[0]

from entity_builder import DocumentEntity

DocumentEntity(data_1)._research_data_item