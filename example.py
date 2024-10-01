from tqdm import tqdm
from wisski.api import Api

import functions
from entity_builder import DocumentEntity

# 1. Upload mongo-data to WissKI

# Fill in the WissKI API username and password below
api = Api("https://www.wisski.uni-bayreuth.de/wisski/api/v0", auth=("username", "password"), headers={"Cache-Control": "no-cache"})
api.pathbuilders = ["amo_ecrm__v01_dev_pb"]

# pathbuilder_save("https://www.wisski.uni-bayreuth.de/sites/default/files/wisski_pathbuilder/export/amo_ecrm__v01_dev_pb_20240821T122919")
data = functions.mongodata_fetch("projects_metadata_ubt", "UBT_DigiRet2022")

# Instantiate a Document entity Class object
uploader = DocumentEntity(api)

# For passing a data object to class
uploader.document(data[0])

# To see staged data before uploading
uploader.staging()

# To uploaded staged data
uploader.uploader()

# Iterate through documents to do multiple uploads
for row in tqdm(data):
    uploader.document(row)
    uploader.upload()

# Run as a loop
for row in tqdm(data[:100]):
    doc = DocumentEntity(api=api, bson_doc=row)
    doc.staging()
    doc.upload()
    
# 2. WissKi entity sync-up
from wisski.api import Api
import entity_sync
from entity_sync import EntitySync

# Sync-up 'institutions' as an example (always update institutions before persons)
institutions = EntitySync(api=api, sync_field='institutions')

# For list of missing entities 
institutions.missing_entities()
 
# For staged Data
institutions.staged()
 
# For uploading
institutions.update()



# 3. WissKi entity Updater
from wisski.api import Api
import functions
import entity_updater
from entity_updater import DocumentUpdate
from entity_builder import DocumentEntity

# Fill in the WissKI API username and password below
api = Api("https://www.wisski.uni-bayreuth.de/wisski/api/v0", auth=("username", "password"), headers={"Cache-Control": "no-cache"})
api.pathbuilders = ["amo_ecrm__v01_dev_pb"]

# Example
updater = DocumentUpdate(
api=api,
dre_identifier="aca-01-0000",
method='subject' or ['citation', 'language', 'subject'],
mongodb="projects_metadata_ubt",
mongocoll="UBT_DigiRet2022"
)

updater.run()
