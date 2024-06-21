# Libraries
import numpy as np

# Data Wrangling
import pandas as pd

# Data Parsing
import json
from datetime import datetime
from urllib.parse import urlparse
import re

# WissKi Api
from wisski.api import Api, Pathbuilder, Entity

# Local Functions
from functions import *
from auth import GeneralEntity
from exception_functions import fieldfunction


# Class for creating the Research Data Item entity

class EasydbEntity(GeneralEntity):

    def __init__(self, entry_dict):
        # Super class
        super().__init__()

        # Metadata Document
        self._document = entry_dict

        # GeoLoc Information
        # ~ self._origin = self._document.get('location').get('origin')

        # Core dictionary for Research Data Items
        self._research_data_item = {

            # Field for Identifiers (Mandatory Field)
            self._bundle.get('g_research_data_item_identifier'): self.identifier_entities()
        }
        
        print(self._research_data_item)

        # Project (Mandatory Field)
        if not self._document.get('context_funding#_standard#de-DE') == '':
            self._research_data_item[self._field.get('f_research_data_item_project')] = [
                entity_uri(self._document.get('context_funding#_standard#de-DE'), self._query.get('projectid'))]
    # Entity list of identifiers

    def identifier_entities(self):

        # Initialising easydb Identifier
        easydb_fields = {
            self._field['f_research_data_item_id_name']: [self._document.get('_global_object_id')],
            self._field['f_research_data_item_id_type']: [entity_uri("easydb Identifier",
                                                                     self._query.get('identifier'))]
        }
        easydb_entity = Entity(api=self._api, fields=easydb_fields,
                              bundle_id=self._bundle['g_research_data_item_identifier'])
        
        # Initialising inventory Identifier
        inventory_fields = {
            self._field['f_research_data_item_id_name']: [self._document.get('registrationnumber')],
            self._field['f_research_data_item_id_type']: [entity_uri("easydb Inventory Number",
                                                                     self._query.get('identifier'))]
        }
        inventory_entity = Entity(api=self._api, fields=inventory_fields,
                              bundle_id=self._bundle['g_research_data_item_identifier'])

        return [easydb_entity,inventory_entity]

    # Citation
    def citation(self):
        if not self._document.get('exploitationrights[].otherlicences') == '':
            self._research_data_item[self._field.get('f_research_data_item_citation')] = self._document.get('exploitationrights[].otherlicences')

    # Geographic Location
    def country(self):
        if not self._document.get('placeoforigin_geographical#_standard#en-US#0') == '':
            self._research_data_item[self._field.get('f_research_data_creat_country')] = [
                entity_uri(
                    search_value=self._document.get('placeoforigin_geographical#_standard#en-US#0'),
                    query_string=self._query.get('country')
                )
            ]

    def get_region_uri(self):
        if not self._document.get('placeoforigin_geographical#_standard#en-US#1') == '':
            region_uri = entity_uri(
                    search_value={'level_0': self._document.get('placeoforigin_geographical#_standard#en-US#0'),
                                  'level_1': self._document.get('placeoforigin_geographical#_standard#en-US#1')},
                    query_string=self._query.get('region'),
                    conditional = True
                )
            return region_uri
        return False

    # Region (level 2)
    def region(self):
        if not self._document.get('placeoforigin_geographical#_standard#en-US#1') == '':
            region_uri = self.get_region_uri()
            if region_uri != False:
                self._research_data_item[self._field.get('f_research_data_item_creat_regio')] = [region_uri]

    # Subregion
    def subregion(self):
        if not self._document.get('placeoforigin_geographical#_standard#en-US#2') == '' and not self._document.get('placeoforigin_geographical#_standard#en-US#1') == '':
            subregion = entity_uri(
                search_value={
                    'level_0': self._document.get('placeoforigin_geographical#_standard#en-US#2'),
                    'level_1': self._document.get('placeoforigin_geographical#_standard#en-US#1')
                },
                query_string=self._query.get('subregion'),
                conditional=True
            )
            if subregion is not None and urlparse(subregion).scheme != '':
                self._research_data_item[self._field.get('f_research_data_item_creat_subre')] = [subregion]
            elif subregion is None:
                self._research_data_item[self._field.get('f_research_data_item_creat_subre')] = [
                    fieldfunction('subregion').exception(entity_value=self._document.get('placeoforigin_geographical#_standard#en-US#2'),
                                                         qualifier_value=self.get_region_uri(),
                                                         with_qualifier=True)
                ]
            
    def place(self):
        if not self._document.get('placeoforigin_details') == '':
            place_str = re.sub(r"^\W+", "", self._document.get('placeoforigin_details'))
            place = entity_uri(
                search_value={
                    'level_0': place_str,
                    'level_1': self._document.get('placeoforigin_geographical#_standard#en-US#0')
                },
                query_string=self._query.get('place'),
                conditional=True
            )
            if place is not None and urlparse(place).scheme != '':
                self._research_data_item[self._field.get('f_research_data_item_create_loc')] = [place]
            elif place is None:
                country_uri = entity_uri(
                    search_value=self._document.get('placeoforigin_geographical#_standard#en-US#0'),
                    query_string=self._query.get('country')
                )
                self._research_data_item[self._field.get('f_research_data_item_create_loc')] = [
                    fieldfunction('place').exception(entity_value=place_str,
                                                     qualifier_value=country_uri,
                                                     with_qualifier=True)
                ]

    # URL
    def url_link(self):
        self._research_data_item[self._field.get('f_research_data_item_url')] = 'https://collections.uni-bayreuth.de/#/detail/' + self._document.get('_global_object_id')

    # Note(s)
    def note(self):
        if not self._document.get('description_simple') == '':
            self._research_data_item[self._field.get('f_research_data_note')] = [self._document.get('description_simple')]
        else:
            pass

    # Associated Person (Mandatory Field)
    def role(self):
        name_entity_list = []
        for name in self._document.get('name'):
            name_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get('f_research_data_item_apers_name'): entity_uri(
                               search_value=name.get('name'),
                               query_string=self._query.get('person')
                           ),
                           self._field.get('f_research_data_item_apers_role'): entity_uri(
                               search_value=name.get('role'),
                               query_string=self._query.get('role')
                           )
                       }, bundle_id=self._bundle.get('g_research_data_item_ass_person'))
            )
        self._research_data_item[self._bundle.get('g_research_data_item_ass_person')] = name_entity_list

    # Title Information
    def titles(self):
        title_entity_list = []
        title_entity_list.append(
            Entity(api=self._api,
                   fields={
                       self._field.get('f_research_data_item_title_appel'): [self._document.get('title#de-DE')],
                       self._field.get('f_research_data_item_title_type'): ['Main']
                   }, bundle_id=self._bundle.get('g_research_data_item_title'))
        )
        title_entity_list.append(
            Entity(api=self._api,
                   fields={
                       self._field.get('f_research_data_item_title_appel'): [self._document.get('alternativetitle#de-DE')],
                       self._field.get('f_research_data_item_title_type'): ['Alternative']
                   }, bundle_id=self._bundle.get('g_research_data_item_title'))
        )
        title_entity_list.append(
            Entity(api=self._api,
                   fields={
                       self._field.get('f_research_data_item_title_appel'): [self._document.get('_pool#de-DE')],
                       self._field.get('f_research_data_item_title_type'): ['Sub']
                   }, bundle_id=self._bundle.get('g_research_data_item_title'))
        )
        self._research_data_item[self._bundle.get('g_research_data_item_title')] = title_entity_list

    # Dates
    def dateinfo(self):
        date_value = datetime.strftime(
                        datetime.fromisoformat(self._document.get('recordingdate')),
                        "%Y-%m-%d")
        self._research_data_item[self._field.get('f_research_data_item_create_date')] = [date_value]

    # Technical Description

    def physicaldesc(self):
        recorder = self._document.get('recorder').lower()
        # digital photography
        if self._document.get('object_type#_global_object_id') == '2@3c910145-7057-4112-9484-5d30f968f4d0' or 'digital' in recorder:
            # Type (Mandatory Field)
            self._research_data_item[self._field.get('f_reseach_data_item_res_type')] = ["Digital"]
            # Method
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_method')] = ["born digital"]
            # Description
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_desc')] = ['digital images','Digital Photography',]
            # Type of Resource (Mandatory Field)
            self._research_data_item[self._field.get('f_research_data_item_type_res')] = [entity_uri("Image", self._query.get('typeofresource'))]
        elif '35mm' in recorder or 'kodachrome' in recorder or 'film' in recorder:
            # Type (Mandatory Field)
            self._research_data_item[self._field.get('f_reseach_data_item_res_type')] = ["Digital"]
            # Method
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_method')] = ["digitized microfilm"]
            # Description
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_desc')] = ['digital images','Digital Photography',]
            # Type of Resource (Mandatory Field)
            self._research_data_item[self._field.get('f_research_data_item_type_res')] = [entity_uri("Image", self._query.get('typeofresource'))]
        elif 'cassette' in recorder or 'kassette' in recorder or 'mp3' in self._document.get('format').lower() or self._document.get('file#0#url').endswith('mp3'):
            # Type (Mandatory Field)
            self._research_data_item[self._field.get('f_reseach_data_item_res_type')] = ["Digital"]
            # Method
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_method')] = ["digitized other analog"]
            # Type of Resource (Mandatory Field)
            self._research_data_item[self._field.get('f_research_data_item_type_res')] = [entity_uri("Audio", self._query.get('typeofresource'))]
        elif self._document.get('file#0#url').endswith('pdf'):
            # Type (Mandatory Field)
            self._research_data_item[self._field.get('f_reseach_data_item_res_type')] = ["Digital"]
            # Method
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_method')] = ["digitized other analog"]
            # Type of Resource (Mandatory Field)
            self._research_data_item[self._field.get('f_research_data_item_type_res')] = [entity_uri("Text", self._query.get('typeofresource'))]
            
        # Technical Property
        self._research_data_item[self._field.get('f_reseach_data_item_tech_prop')] = self._document.get('colourbw')
        # Technical Description Note
        self._research_data_item[self._field.get('f_reseach_data_item_res_t_descr')] = self._document.get('recorder')

    def genre(self):
        genre_entities = []
        authority_uri = entity_uri(search_value='Machine-Readable Cataloging',
                                       query_string=self._query.get('identifier'))
        
        term_uri = entity_uri(search_value={'term': 'picture', 'authority': authority_uri.split('data/')[1]},
                              query_string=self._query.get('genre'),
                              conditional=True)
        if term_uri is None:
            genre_entities.append(fieldfunction('genre').exception(
                entity_value='picture',
                qualifier_value=authority_uri,
                with_qualifier=True
            ))
        elif urlparse(term_uri).scheme != '':
            genre_entities.append(term_uri)
        self._research_data_item[self._field.get('f_research_data_item_auth_tag')] = genre_entities

    # Tags
    # TODO: Tags Values
    def tags(self):
        values = []
        if not self._document.get('keywords[].keyword#de-DE') == '':
            values.extend(self._document.get('keywords[].keyword#de-DE').split('\n'))
        if not self._document.get('keywords_gnd[].keyword_gnd#_standard') == '':
            values.extend(self._document.get('keywords_gnd[].keyword_gnd#_standard').split('\n'))
        if not self._document.get('keywords_aat[].keyword_aat#_standard') == '':
            values.extend(self._document.get('keywords_aat[].keyword_aat#_standard').split('\n'))
        if len(values) > 0:
            self._research_data_item[self._field.get('f_reseach_data_item_tag')] = entity_list_generate(
                value_list=values,
                query_name=self._query.get('tags'),
                exception_function=fieldfunction('tags').exception,
                with_exception=True
            )

    # Staged Values

    def staging(self):
        self.citation()
        self.url_link()
        self.note()
        self.genre()
        self.country()
        self.region()
        self.subregion()
        self.place()
        # ~ #self.role()
        self.titles()
        self.dateinfo()
        self.physicaldesc()
        self.tags()
        return self._research_data_item

    def upload(self):
        self._api.save(Entity(
            api=self._api,
            fields=self.staging(),
            bundle_id=self._bundle.get('g_research_data_item')
        ))


# Person Entity Synchronisation


class EntitySync(GeneralEntity):

    def __init__(self, sync_field, check_list):

        # Field name initialisation
        self._sync_field_name = sync_field
        self._list = check_list

        # Super Class
        super().__init__()

        # WissKI Query Dict
        self._wisski_query = {
            "persons": "personlist",
            "institutions": "institutionlist"
        }

        # WissKI Path field Dict

        self._wisski_path_field = {
            "persons": "f_person_name",
            "institutions": "f_institution_name",
        }

        # WissKI Path Group Dict

        self._wisski_path_group = {
            "persons": "g_person",
            "institutions": "g_institution"
        }

        # WissKI Data
        self._wisski_entities = list(entity_uri(search_value="",
                                                query_string=self._query.get(self._wisski_query[self._sync_field_name]),
                                                return_format='csv', value_input=False).iloc[:, 0])

    def check_missing(self):
        missing_values = []
        for value in self._list:
            if value not in self._wisski_entities:
                missing_values.append(value)
        return missing_values

    def update(self):
        missing = self.check_missing()
        if not missing == []:
            for entity in missing:
                entity_value = {
                    self._field.get(self._wisski_path_field.get(self._sync_field_name)): [entity]
                }
                entity_object = Entity(api=self._api, fields=entity_value,
                                       bundle_id=self._bundle.get(self._wisski_path_group.get(self._sync_field_name)))
                self._api.save(entity_object)
