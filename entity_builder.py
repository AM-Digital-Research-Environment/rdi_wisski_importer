# Libraries

# MongoDB
from pymongo import MongoClient

# Data Wrangling
import pandas as pd

# Data Parsing
import json

# WissKi Api
from wisski.api import Api, Pathbuilder, Entity

# Local Functions
from functions import entity_uri, json_file, entity_list_generate

# Class for creating the Research Data Item entity


class DocumentEntity:

    def __init__(self, bson_doc):
        self._document = bson_doc
        self._bundle = json_file("dicts/bundles.json")
        self._field = json_file("dicts/fields.json")
        self._query = json_file("dicts/sparql_queries.json")
        self._api_url = "***REMOVED***/wisski/api/v0"
        self._auth = ("***REMOVED***", "***REMOVED***")
        self._api = Api(self._api_url, self._auth, {"Cache-Control": "no-cache"})
        self._api.pathbuilders = ["amo_ecrm__v01_dev_pb"]
        # Core dictionary for Research Data Items
        self._research_data_item = {
            # Type of Resource (Mandatory Field)
            self._field.get('f_research_data_item_type_res'): [entity_uri(self._document.get('typeOfResource'),
                                                                      self._query.get('typeofresource'))],
            # Field for Identifiers (Mandatory Field)
            self._bundle.get('g_research_data_item_identifier'): self.identifier_entities(),
            # Sponsorship (Mandatory Field)
            self._field.get('f_research_data_item_sponsor'): self._document.get('sponsor')
            # Citation

        }
    # Entity list of identifiers

    def identifier_entities(self):
        entity_list = []
        for iden in self._document.get('identifier'):
            identifier_fields = {
                self._field['f_research_data_item_id_name']: [iden.get('identifier')],
                self._field['f_research_data_item_id_type']: [entity_uri(iden.get('identifier_type'),
                                                                         self._query.get('identifier'))]
            }
            identifier_entity = Entity(api=self._api,
                                       fields=identifier_fields,
                                       bundle_id=self._bundle['g_research_data_item_identifier'])
            entity_list.append(identifier_entity)
        return entity_list

    # Language
    def langauge(self):
        if not self._document.get('langauge') == []:
            self._research_data_item[self._field.get('f_research_data_item_language')] = entity_list_generate(
                self._document.get('language'),
                self._query.get('language')
            )
        else:
            pass

    # Citation
    def citation(self):
        if not self._document.get(citation) == []:
            self._research_data_item[self._field.get('f_research_data_item_citation')] = self._document.get('citation')
        else:
            pass

    # Staged Values
    def staging(self):
        self.langauge()
        self.citation()
        return self._research_data_item