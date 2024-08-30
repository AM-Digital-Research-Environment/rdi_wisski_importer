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
from functions import entity_uri, mongodata_fetch


class DocumentUpdate(DocumentEntity):

    def __init__(self, api: Api, dre_identifier: str, method: str, mongodb: str, mongocoll: str):

        # Local initialisation
        self._api = api
        self._id = dre_identifier
        self._bson_doc = list(
            mongodata_fetch(db_name=mongodb, collection_name=mongocoll, as_list=False).find({"dre_id": self._id})
        )[0]
        self._method = method

        # Super class initialisation
        super().__init__(bson_doc=self._bson_doc, api=self._api, return_value=True)

        # Research entity to be edited
        self._edit_entity = self._api.get_entity(
            entity_uri(search_value=self._id, query_string=self._query.get('dreID'))
        )



    # Staged Data
    def check(self):

        match self._method:
            case 'physicalDesc':
                return self.physicaldesc()

    # Method match case method
    def run(self, value_append: bool = False, new_value=None):

            case 'physicalDesc':
                bundle_id = self._bundle.get('g_reseach_data_item_res_type')
                base = self._edit_entity.fields[bundle_id]
                if value_append:
                    self._edit_entity.fields[bundle_id] = base.append(new_value)
                else:
                    self._edit_entity.fields[bundle_id] = self.physicaldesc()
                self._api.save(self._edit_entity)

