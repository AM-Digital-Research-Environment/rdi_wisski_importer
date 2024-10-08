# Research Data Item Importer
Repo for data insertion into WissKI Main Instance (VM 89)

### Authentication for the MongoDB Client and the GraphDB connection
For authentication information (MongoDB Client bot URI and functions_config.json) in the functions.py, please fill in or enquire us.

**UNDER MAINTENANCE**

## API configuration

### Inject an API instance into `DocumentEntity`

To avoid internal setup routines from running over and over again, we expect a pre-configured instance of `wisski_py.Api` to be injected into the `DocumentEntity`:

```python
# Instantiating api object
api = Api(
    "https://www.wisski.com/api/v0",
    auth=("your_wisski_username", "your_wisski_password"),
    headers={"Cache-Control": "no-cache"}
    )
api.pathbuilders = ["pathbuilder_of_choice"]

# Fetching you data from source (in this case, MongoDB)
data = mongodata_fetch("projects_metadata_ubt", "UBT_DigiRet2022")

# Instantiate a Document entity Class object
uploader = DocumentEntity(api)

# For passing a data object to class
uploader.document(data[0])

# To see staged data before uploading
uploader.staging()

# To uploaded staged data
uploader.upload()

# Iterate through documents to do multiple uploads
for row in data:
    uploader.document(row)
    uploader.upload()
```

Also see [the example](example.py).


### Pathbuilders

Check [the example](example.py) for how to set up the wisski_py API wrapper, and fetch data to be inserted. 

Note that the wisski_py wrapper needs to either be told to use **all** available pathbuilders by calling `api.init_pathbuilders()`, or configured explicitly with the pathbuilders to use:


```python
# Check which pathbuilders are present in the system.
print(api.get_pathbuilder_ids()) 
# >>> ['pathbuilder1', 'pathbuilder2', 'linkblock_pathbuilder']

# Initialize all available pathbuilders:
api.init_pathbuilders()

# Or configure the pathbuilder explicitly; this internalizes the pathbuilder under
# the hood, no further processing is required:
api.pathbuilders = ['pathbuilder1']
```
