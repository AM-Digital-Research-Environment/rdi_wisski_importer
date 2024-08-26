# Summary
"""
This class file holds the class used for updating pre-existing WissKI enities
by specified field. The reference to each entity will be made using
DRE Identifier (all fields can be updated, with the exception of identifiers)
"""

# Pseudo-Code
"""
Input: DRE Identifier (string), field name (string)
Process:
The class wil use the update the specified field.
Ouput: Success report
"""

# Libraries
from entity_builder import DocumentEntity
from wisski.api import Api, Entity
from functions import entity_uri


class DocumentUpdate():

    def __init__(self, bson_doc, api: Api, dre_identifier, fieldMethod):
        pass
