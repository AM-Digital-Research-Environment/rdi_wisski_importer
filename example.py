
from tqdm import tqdm
from wisski.api import Api

import functions
from entity_builder import DocumentEntity

# Fill in the username and password below
username = ""
password = ""
api = Api("https://www.wisski.uni-bayreuth.de/wisski/api/v0", auth=(username, password), headers={"Cache-Control": "no-cache"})
api.pathbuilders = ["amo_ecrm__v01_dev_pb"]

# pathbuilder_save("https://www.wisski.uni-bayreuth.de/sites/default/files/wisski_pathbuilder/export/amo_ecrm__v01_dev_pb_20240821T122919")
data = functions.mongodata_fetch("projects_metadata_ubt", "UBT_DigiRet2022")
for row in tqdm(data[:100]):
    staged = DocumentEntity(row, api).staging()

    uploaded = DocumentEntity(row, api).upload()
