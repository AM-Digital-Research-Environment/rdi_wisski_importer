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
import functions
from auth import GeneralEntity
from exception_functions import fieldfunction


# Class for creating the Research Data Item entity

class EasydbEntity(GeneralEntity):

    def __init__(self, entry_dict, known_objects=None, location_mappings=None, api=None, known_entities=None):
        # Super class
        super().__init__(api, known_entities)
        
        self.entity_lookups = {}

        # Metadata Document
        self._document = entry_dict
        self.api = api
        self.known_entities = known_entities
        
        # Mapping of Location Names
        self.location_mappings = location_mappings
        
        if self._document.get('_global_object_id').strip() not in known_objects:
            self.already_in_database = False
        
            # Core dictionary for Research Data Items
            self._research_data_item = {

                # Field for Identifiers (Mandatory Field)
                self._bundle.get('g_research_data_item_identifier'): self.identifier_entities()
            }
            
            # Project (Mandatory Field)
            if self._document.get('context_funding#_standard#de-DE') != '':
                self._research_data_item[self._field.get('f_research_data_item_project')] = [
                    self.entity_uri(self._document.get('context_funding#_standard#de-DE'), 'projectname')]
        else:
            self.already_in_database = True
    
    def get_already_in_db(self):
        return self.already_in_database
    
    # Entity list of identifiers

    def identifier_entities(self):
        # Initialising easydb Identifier
        easydb_fields = {
            self._field['f_research_data_item_id_name']: [self._document.get('_global_object_id')],
            self._field['f_research_data_item_id_type']: [self.entity_uri("Publisher, distributor, or vendor stock number",'identifier')]
        }
        easydb_entity = Entity(api=self._api, fields=easydb_fields,
                              bundle_id=self._bundle['g_research_data_item_identifier'])
        
        # Initialising inventory Identifier
        inventory_fields = {
            self._field['f_research_data_item_id_name']: [self._document.get('registrationnumber')],
            self._field['f_research_data_item_id_type']: [self.entity_uri("Locally defined identifier",'identifier')]
        }
        inventory_entity = Entity(api=self._api, fields=inventory_fields,
                              bundle_id=self._bundle['g_research_data_item_identifier'])
        return [easydb_entity,inventory_entity]

    # Citation
    def citation(self):
        if not self._document.get('exploitationrights[].otherlicences') == '':
            self._research_data_item[self._field.get('f_research_data_item_citation')] = [self._document.get('exploitationrights[].otherlicences')]
    
    # Geographic Location
    def location(self):
        if self._document.get('placeoforigin_geographical#_system_object_id') != '' and int(self._document.get('placeoforigin_geographical#_system_object_id')) in self.location_mappings.keys():
            mapping = self.location_mappings[int(self._document.get('placeoforigin_geographical#_system_object_id'))]
            # Region (level 0)
            if 'country' in mapping:
                country_uri = self.entity_uri(
                        search_value=mapping['country'],
                        query_id='country'
                    )
                self._research_data_item[self._field.get('f_research_data_creat_country')] = [country_uri]
                # Region (level 1)
                region_uri = None
                subregion_uri = None
                if 'region' in mapping:
                    region_uri = self.entity_uri(
                            search_value={'level_1': mapping['country'],
                                          'level_0': mapping['region']},
                            query_id='region',
                            conditional = True
                        )
                    self._research_data_item[self._field.get('f_research_data_item_creat_regio')] = [region_uri]
                    
                    # Subegion (level 2)
                    if 'subregion' in mapping:
                        subregion_uri = self.entity_uri(
                            search_value={
                                'level_0': mapping['subregion'],
                                'level_1': mapping['region']
                            },
                            query_id='subregion',
                            conditional=True
                        )
                        if subregion_uri is not None and urlparse(subregion_uri).scheme != '':
                            self._research_data_item[self._field.get('f_research_data_item_creat_subre')] = [subregion_uri]
                        elif subregion_uri is None:
                            subregion_uri = fieldfunction('subregion', self.api, self.known_entities).exception(entity_value=mapping['subregion'],
                                                                     qualifier_value=region_uri,
                                                                     with_qualifier=True)
                            self._research_data_item[self._field.get('f_research_data_item_creat_subre')] = [subregion_uri]
                
                # ~ if 'place' in mapping:
                    # ~ place = self.entity_uri(
                        # ~ search_value={
                            # ~ 'level_0': mapping['place'],
                            # ~ 'level_1': mapping['country']
                        # ~ },
                        # ~ query_id='place',
                        # ~ conditional=True
                    # ~ )
                    # ~ if place is not None and urlparse(place).scheme != '':
                        # ~ self._research_data_item[self._field.get('f_research_data_item_create_loc')] = [place]
                    # ~ elif place is None:
                        # ~ print("try adding place", country_uri, region_uri, subregion_uri)
                        # ~ place_uri = fieldfunction('place', self.api, self.known_entities).exception(entity_value=mapping['place'],
                                                             # ~ qualifier_value=country_uri,
                                                             # ~ qualifier2=region_uri,
                                                             # ~ qualifier3=subregion_uri,
                                                             # ~ with_qualifier=True)
                        # ~ print(place_uri)
                        # ~ self._research_data_item[self._field.get('f_research_data_item_create_loc')] = [place_uri]

    # URL
    def url_link(self):
        self._research_data_item[self._field.get('f_research_data_item_url')] = ['https://collections.uni-bayreuth.de/#/detail/' + self._document.get('_global_object_id')]
        
    # URL
    def preview_image_url(self):
        if self._document.get('file#2#url') != '':
            self._research_data_item[self._field.get('f_research_data_item_prv_img_url')] = [self._document.get('file#2#url')]

    # Note(s)
    def note(self):
        if not self._document.get('description_simple') == '':
            self._research_data_item[self._field.get('f_research_data_note')] = [self._document.get('description_simple')]

    # Associated Person (Mandatory Field)
    def role(self):
        name_entity_list = []
        name_entity_list.append(
            Entity(api=self._api,
                   fields={
                       self._field.get('f_research_data_item_role_hldr_i'): [self.entity_uri(
                           search_value='Collections@UBT',
                           query_id='institution'
                       )],
                       self._field.get('f_research_data_item_apers_role'): [self.entity_uri(
                           search_value='Publisher',
                           query_id='role'
                       )]
                   }, bundle_id=self._bundle.get('g_research_data_item_ass_person'))
        )
        if self._document.get('creator#_standard#de-DE') != '':
            name_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get('f_research_data_item_role_holder'): [self.entity_uri(
                               search_value=self._document.get('creator#_standard#de-DE'),
                               query_id='person'
                           )],
                           self._field.get('f_research_data_item_apers_role'): [self.entity_uri(
                               search_value='Creator',
                               query_id='role'
                           )]
                       }, bundle_id=self._bundle.get('g_research_data_item_ass_person'))
            )

        self._research_data_item[self._bundle.get('g_research_data_item_ass_person')] = name_entity_list

    # Title Information
    def titles(self):
        title_entity_list = []
        if self._document.get('title#de-DE') != '':
            title_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get('f_research_data_item_title_appel'): [self._document.get('title#de-DE')],
                           self._field.get('f_research_data_item_title_type'): ['Main']
                       }, bundle_id=self._bundle.get('g_research_data_item_title'))
            )
        if self._document.get('alternativetitle#de-DE') != '':
            title_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get('f_research_data_item_title_appel'): [self._document.get('alternativetitle#de-DE')],
                           self._field.get('f_research_data_item_title_type'): ['Alternative']
                       }, bundle_id=self._bundle.get('g_research_data_item_title'))
            )
        if self._document.get('_pool#de-DE') != '':
            title_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get('f_research_data_item_title_appel'): [self._document.get('_pool#de-DE')],
                           self._field.get('f_research_data_item_title_type'): ['Sub']
                       }, bundle_id=self._bundle.get('g_research_data_item_title'))
            )
        if len(title_entity_list) == 0:
            return False
        self._research_data_item[self._bundle.get('g_research_data_item_title')] = title_entity_list
        return True

    # Dates
    def dateinfo(self):
        date_value = None
        try:
            date_value = datetime.strftime(
                            datetime.fromisoformat(self._document.get('recordingdate')),
                            "%Y-%m-%d")
        except ValueError:
            try:
                date_value = datetime.strftime(
                                datetime.strptime(self._document.get('recordingdate'), '%Y-%m'),
                                "%Y-%m-01")
            except ValueError:
                pass
        if date_value is not None:
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
            self._research_data_item[self._field.get('f_research_data_item_type_res')] = [self.entity_uri("Image", 'typeofresource')]
        elif '35mm' in recorder or 'kodachrome' in recorder or 'film' in recorder:
            # Type (Mandatory Field)
            self._research_data_item[self._field.get('f_reseach_data_item_res_type')] = ["Digital"]
            # Method
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_method')] = ["digitized microfilm"]
            # Description
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_desc')] = ['digital images','Digital Photography',]
            # Type of Resource (Mandatory Field)
            self._research_data_item[self._field.get('f_research_data_item_type_res')] = [self.entity_uri("Image", 'typeofresource')]
        elif 'cassette' in recorder or 'kassette' in recorder or 'mp3' in self._document.get('format').lower() or self._document.get('file#0#url').endswith('mp3'):
            # Type (Mandatory Field)
            self._research_data_item[self._field.get('f_reseach_data_item_res_type')] = ["Digital"]
            # Method
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_method')] = ["digitized other analog"]
            # Type of Resource (Mandatory Field)
            self._research_data_item[self._field.get('f_research_data_item_type_res')] = [self.entity_uri("Audio", 'typeofresource')]
        elif self._document.get('file#0#url').endswith('pdf'):
            # Type (Mandatory Field)
            self._research_data_item[self._field.get('f_reseach_data_item_res_type')] = ["Digital"]
            # Method
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_method')] = ["digitized other analog"]
            # Type of Resource (Mandatory Field)
            self._research_data_item[self._field.get('f_research_data_item_type_res')] = [self.entity_uri("Text", 'typeofresource')]
            
        # Technical Property
        self._research_data_item[self._field.get('f_reseach_data_item_tech_prop')] = [self._document.get('colourbw')]
        # Technical Description Note
        self._research_data_item[self._field.get('f_reseach_data_item_res_t_descr')] = [self._document.get('recorder')]

    def genre(self):
        genre_entities = []
        authority_uri = self.entity_uri(search_value='Machine-Readable Cataloging',
                                       query_id='identifier')
        
        term_uri = self.entity_uri(search_value={'term': 'picture', 'authority': authority_uri.split('data/')[1]},
                              query_id='genre',
                              conditional=True)
        if term_uri is None:
            genre_entities.append(fieldfunction('genre', self.api, self.known_entities).exception(
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
        if self._document.get('keywords[].keyword#de-DE') != '':
            values.extend(self._document.get('keywords[].keyword#de-DE').split('\n'))
        if self._document.get('keywords_gnd[].keyword_gnd#_standard') != '':
            values.extend(self._document.get('keywords_gnd[].keyword_gnd#_standard').split('\n'))
        if self._document.get('keywords_aat[].keyword_aat#_standard') != '':
            values.extend(self._document.get('keywords_aat[].keyword_aat#_standard').split('\n'))
        if self._document.get('_pool#de-DE') != '':
            values.append(self._document.get('_pool#de-DE'))
        if len(values) > 0:
            self._research_data_item[self._field.get('f_reseach_data_item_tag')] = self.entity_list_generate(
                value_list=values,
                query_name='tags',
                exception_function=fieldfunction('tags', self.api, self.known_entities).exception,
                with_exception=True
            )

    # Staged Values

    def staging(self):
        if not self.already_in_database and self.titles():
            self.citation()
            self.url_link()
            self.note()
            self.genre()
            self.location()
            self.preview_image_url()
            self.role()
            self.dateinfo()
            self.physicaldesc()
            self.tags()
            
            # not working yet
            # ~ self.place()
            # ~ print(self._research_data_item)
            return self._research_data_item
        else:
            return False

    def upload(self):
        stage = self.staging()
        if stage:
            self._api.save(Entity(
                api=self._api,
                fields=stage,
                bundle_id=self._bundle.get('g_research_data_item')
            ))
            return True
        else:
            return False


# Person Entity Synchronisation


class EntitySync(GeneralEntity):

    def __init__(self, sync_field, check_list, api=None, known_entities=None):

        # Field name initialisation
        self._sync_field_name = sync_field
        self._list = check_list

        # Super Class
        super().__init__(api, known_entities)

        # WissKI Query Dict
        self._wisski_query = {
            "authorityroles": "authorityrolelist",
            "persons": "personlist",
            "institutions": "institutionlist",
            "identifiers": "identifierlist",
            "projects": "projectlist",
            "easydb_objects": "easydb_ids"
        }

        # WissKI Path field Dict

        self._wisski_path_field = {
            "authorityroles": "f_authority_role_name",
            "persons": "f_person_name",
            "institutions": "f_institution_name",
            "identifiers": "f_authority_name",
            "projects": "f_project_name",
        }

        # WissKI Path Group Dict

        self._wisski_path_group = {
            "authorityroles": "g_authority_role",
            "persons": "g_person",
            "institutions": "g_institution",
            "identifiers": "g_authority",
            "projects": "g_project",
        }

        # WissKI Data
        self._wisski_entities = list(functions.entity_uri(search_value="",
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
                print(entity)
                entity_value = {
                    self._field.get(self._wisski_path_field.get(self._sync_field_name)): [entity]
                }
                entity_object = Entity(api=self._api, fields=entity_value,
                                       bundle_id=self._bundle.get(self._wisski_path_group.get(self._sync_field_name)))
                self._api.save(entity_object)
    
    def update_multiple_values(self, values, lookup_key):
        missing = self.check_missing()
        if not missing == []:
            for entity in missing:
                values_for_update = next((item for item in values if values[lookup_key] == entity), False)
                if not values_for_update:
                    continue
                entity_values = {}
                for k in values_for_update.keys():
                    entity_values[self._field.get(k)]= [values_for_update[k]]
                entity_object = Entity(api=self._api, fields=entity_values,
                                       bundle_id=self._bundle.get(self._wisski_path_group.get(self._sync_field_name)))
                print(entity_object)
                self._api.save(entity_object)
