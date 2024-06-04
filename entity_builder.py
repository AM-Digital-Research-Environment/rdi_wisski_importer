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
from functions import entity_uri, json_file

# Class for creating the Research Data Item entity


class DocumentEntity:

    def __init__(self, bson_doc):
        self._document = bson_doc
        self._bundle = json_file("dicts/bundles.json")
        self._field = json_file("dicts/fields.json")
        self._query = json_file("dicts/sparql_queries.json")

    # Core dictionary for Research Data Item
    def research_data_item(self):
        return {
            #Type of Resource
            self._field['f_research_data_item_type_res']: [entity_uri(self._document['typeOfResource'],
                                                                      'TypeOfResource',
                                                                      'json')],
            # Field for Identifiers
            self._bundle['g_research_data_item_identifier']: self.identifier_entities(),

        }
    # Entity list of identifiers

    def identifier_entities(self):
        entity_list = []
        for iden in self._document.identifier:
            identifier_fields = {
                self._field['f_research_data_item_id_name']: [iden.identifier],
                self._field['f_research_data_item_id_type']: [entity_uri(iden.identifier_type,
                                                                         'identifier',
                                                                         'json')]
            }
            identifier_entity = Entity(api=api,
                                       fields=identifier_fields,
                                       bundle_id=self._bundle['g_research_data_item_identifier'])
            entity_list.append(identifier_entity)
        return entity_list

