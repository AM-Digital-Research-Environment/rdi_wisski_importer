# Research Data Item Importer
Repo for data insertion into WissKI Main Instance (VM 89)

The following will illustration the steps required to upload the metadata documents strong on DRE MongoDB temp. database to the WissKI instnace on the VM 89.

### Preliminary steps:

- Synchronise person and institution entities between MongoDB and WissKI. To ensure the values are up-to-date folow the steps given below.  
You can use the 'EntitySync' class in the entity_builder package to update the said values.

~~~
# Import EntitySync
from entity_builder import EntitySync

# Instantiate En
~~~