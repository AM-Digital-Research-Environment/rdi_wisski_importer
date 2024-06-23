# Local Functions
import functions
from functions import json_file
from typing import Callable

# WissKi Api
from wisski.api import Api, Pathbuilder, Entity

import json

# General entity management variable class


class GeneralEntity:

    def __init__(self, api = None, known_entities = None):

        # Dictionary Objects
        self._bundle = json_file("dicts/bundles.json")
        self._field = json_file("dicts/fields.json")
        self._query = json_file("dicts/sparql_queries.json")
        self._language = json_file("dicts/lang.json")

        # WissKI Auth
        if api is None:
                self._api_url = "***REMOVED***/wisski/api/v0"
                self._auth = ("***REMOVED***", "***REMOVED***")
                self._api = Api(self._api_url, self._auth, {"Cache-Control": "no-cache"})
                self._api.pathbuilders = ["amo_ecrm__v01_dev_pb"]
        else:
                self._api = api
        
        if known_entities is None:
                self.wisski_entities = {}
        else:
                self.wisski_entities = known_entities
        
    def get_known_entities(self):
            return self.wisski_entities
        
    def get_api(self):
            return self._api
    
    
    def entity_uri(self, search_value: str | dict[str, str],
               query_id: str,
               return_format='json',
               value_input=True,
               conditional=False) -> str | object | None:
                       
        if type(search_value) == str:               
                _entity_key = search_value
        elif type(search_value) == dict:
                _entity_key = '|'.join(search_value.values())
        
        if query_id not in self.wisski_entities or _entity_key not in self.wisski_entities[query_id]:
                q = self._query.get(query_id)
                if query_id not in self.wisski_entities:
                        self.wisski_entities[query_id] = {}
                
                entity = functions.entity_uri(search_value=search_value, query_string=q, return_format=return_format, value_input=value_input, conditional=conditional)
                self.wisski_entities[query_id][_entity_key] = entity
                # ~ print("had to update known entities for",query_id)
        # ~ print(json.dumps(self.wisski_entities, indent=2))
        return self.wisski_entities[query_id][_entity_key]
     
    def entity_list_generate(self, value_list, query_name, exception_function: Callable, with_exception=False):
        entity_list = []
        for entity_value in value_list:
                uri_value = self.entity_uri(entity_value, query_name)
                if uri_value is None:
                    if with_exception:
                        entity_list.append(exception_function(entity_value=entity_value))
                    elif not with_exception:
                        entity_list.append(entity_value)
                else:
                    entity_list.append(uri_value)
        return entity_list
