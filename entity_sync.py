# Data Wrangling
import pandas as pd

# Data Parsing
import jmespath as jm

# Local Libraries
from auth import *
from functions import *

# WissKI
from wisski.api import Api, Entity

# Class for Associated Entities Synchronisation


class EntitySync(GeneralEntity):
    """
    PSEUDO-CODE

    - List of mongo entities
    - List of WissKI bundle entities
    - Compare function
    - Check function/ Staged function
    - Update function

    """

    def __init__(self, api: Api, sync_field: str):

        # API-client
        self._api = api

        # Field name initialisation
        self._sync_field_name = sync_field
        self._collection = mongodata_fetch(db_name='dev', collection_name=self._sync_field_name)
        self._single_fields = ['institutions', 'groups']
        self._double_fields = ['persons', 'collections']

        # Super Class
        super().__init__()

        # Bundle dictionary
        self._bundle_dict = {
            "institutions": {
                "query": "institutionlist",
                "field": "f_institution_name",
                "group": "g_institution"
            },
            "persons": {
                "query": "personlist",
                "field": "f_person_name",
                "alt_field": "f_person_affiliation",
                "alt_field_label": "affiliation",
                "group": "g_person"
            },
            "groups": {
                "query": "grouplist",
                "field": "f_group_name",
                "group": "g_group"
            },
            "collections": {
                "query": "collectionlist",
                "field": "f_collection_title",
                "alt_field": "f_collection_identifier",
                "alt_field_label": "identifier",
                "group": "g_collection"
            }
        }

    # WissKI Entity List

    def wisski_list(self):
        return list(entity_uri(search_value="",
                               query_string=self._query.get(self._bundle_dict.get(self._sync_field_name)['query']),
                               return_format='csv', value_input=False).iloc[:, 0])

    def missing_entities(self):
        missing = []
        wisskiList = self.wisski_list()
        mongo_response = self._collection
        mongoList = jm.search("[].name", mongo_response)
        for entity in mongoList:
            if entity not in wisskiList:
                missing.append(jm.search(f"[?name == '{entity}']", mongo_response)[0])
            else:
                pass
        return missing

    def staged(self):
        entity_values_list = []
        entity_list = []
        field_dict = self._bundle_dict.get(self._sync_field_name)
        for entity in self.missing_entities():
            entity_value = {
                self._field.get(field_dict.get('field')): [entity.get("name")]
            }
            if self._sync_field_name in self._double_fields:
                if self._sync_field_name == 'persons':
                    if entity.get(field_dict.get('alt_field_label')):
                        entity_value[self._field.get(field_dict.get('alt_field'))] = entity_list_generate(
                            value_list=entity.get(field_dict.get('alt_field_label')),
                            query_name=self._query.get('institution')
                        )
                else:
                    entity_value[self._field.get(field_dict.get('alt_field'))] = [
                        entity.get(field_dict.get('alt_field_label'))
                    ]
            else:
                pass

            # Appending list of Entity Values
            entity_values_list.append(entity_value)

            # Creating entity objects
            entity_object = Entity(
                api=self._api,
                fields=entity_value,
                bundle_id=self._bundle.get(field_dict.get('group'))
            )

            # Appending entity object list
            entity_list.append(entity_object)

        return {'entities': entity_list, 'values': entity_values_list}

    # Update function
    def update(self):
        for entity_obj in self.staged()['entities']:
            self._api.save(entity_obj)

    # TODO: Person entity affiliation the synchronise
    """
    In the scenario where a pre-existing person entity
    has new affiliation to be updated. This function must
    check and update entities with new values. (This must be
    done once institution list is up-to-date)
    """

    def affiliations_update(self):
        pass

    # TODO: Related Items
    """
    Update reaserch data items with related items link.
    """