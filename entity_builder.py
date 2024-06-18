# Libraries

# MongoDB
from pymongo import MongoClient

# Data Wrangling
import pandas as pd

# Data Parsing
import json
from urllib.parse import urlparse

# WissKi Api
from wisski.api import Api, Pathbuilder, Entity

# Local Functions
from functions import entity_uri, json_file, entity_list_generate

# General entity management variable class


class GeneralEntity:

    def __init__(self):

        # Dictionary Objects
        self._bundle = json_file("dicts/bundles.json")
        self._field = json_file("dicts/fields.json")
        self._query = json_file("dicts/sparql_queries.json")
        self._language = json_file("dicts/lang.json")

        # WissKI Auth
        self._api_url = "http://132.180.10.89/wisski/api/v0"
        self._auth = ("DataManager", "1618931-Multiple-2024")
        self._api = Api(self._api_url, self._auth, {"Cache-Control": "no-cache"})
        self._api.pathbuilders = ["amo_ecrm__v01_dev_pb"]


# Class for creating the Research Data Item entity

class DocumentEntity(GeneralEntity):

    def __init__(self, bson_doc):

        # Super class
        super().__init__()

        # BSON Metadata Document
        self._document = bson_doc

        # GeoLoc Information
        self._origin = self._document.get('location').get('origin')

        # Core dictionary for Research Data Items
        self._research_data_item = {

            # Type of Resource (Mandatory Field)
            self._field.get('f_research_data_item_type_res'): [
                entity_uri(self._document.get('typeOfResource'), self._query.get('typeofresource'))],

            # Field for Identifiers (Mandatory Field)
            self._bundle.get('g_research_data_item_identifier'): self.identifier_entities(),

            # Sponsorship (Mandatory Field)
            self._field.get('f_research_data_item_sponsor'): self._document.get('sponsor'),

            # Project (Mandatory Field)
            self._field.get('f_research_data_item_project'): [
                entity_uri(self._document.get('project')['id'], self._query.get('projectid'))]
        }
    # Entity list of identifiers

    def identifier_entities(self):

        # Initialising DRE Identifier
        dreId_fields = {
            self._field['f_research_data_item_id_name']: [self._document.get('dre_id')],
            self._field['f_research_data_item_id_type']: [entity_uri("DRE Identifier", self._query.get('identifier'))]
        }
        dreId_entity = Entity(api=self._api, fields=dreId_fields,
                              bundle_id=self._bundle['g_research_data_item_identifier'])

        entity_list = [dreId_entity]

        # Remaining Standard Identifiers
        for iden in self._document.get('identifier'):
            identifier_fields = {
                self._field['f_research_data_item_id_name']: [iden.get('identifier')],
                self._field['f_research_data_item_id_type']: [
                    entity_uri(iden.get('identifier_type'), self._query.get('identifier'))]
            }
            identifier_entity = Entity(api=self._api,
                                       fields=identifier_fields,
                                       bundle_id=self._bundle['g_research_data_item_identifier'])
            entity_list.append(identifier_entity)
        return entity_list

    # Language
    def langauge(self):
        if not self._document.get('langauge') == []:
            document_languages = [self._language.get(l) for l in self._document.get('language')]
            self._research_data_item[self._field.get('f_research_data_item_language')] = entity_list_generate(
                document_languages,
                self._query.get('language')
            )
        else:
            pass

    # Citation
    def citation(self):
        if not self._document.get('citation') == []:
            self._research_data_item[self._field.get('f_research_data_item_citation')] = self._document.get('citation')
        else:
            pass

    # Geographic Location
    # Country (level 1)
    def country(self):
        if not pd.isna(self._origin.get('l1')):
            self._research_data_item[self._field.get('f_research_data_creat_country')]=[
                entity_uri(
                    search_value=self._origin.get('l1'),
                    query_string=self._query.get('country')
                )
            ]
        else:
            pass

    # Region (level 2)
    def region(self):
        if not pd.isna(self._origin.get('l2')):
            region_uri = entity_uri(
                search_value={'level_0': self._origin.get('l2'),
                              'level_1': self._origin.get('l1')},
                query_string=self._query.get('region'),
                conditional=True)
            self._research_data_item[self._field.get('f_research_data_item_creat_regio')] = [region_uri]
            return region_uri
        else:
            pass

    # Subregion
    def subregion(self):
        if not pd.isna(self._origin.get('l3')):
            subregion = entity_uri(
                search_value={
                    'level_0': self._origin.get('l3'),
                    'level_1': self._origin.get('l2')
                },
                query_string=self._query.get('subregion'),
                conditional=True
            )
            if subregion is not None and urlparse(subregion).scheme != '':
                self._research_data_item[self._field.get('f_research_data_item_creat_subre')] = [subregion]
            elif subregion is None:
                self._research_data_item[self._field.get('f_research_data_item_creat_subre')] = [
                    Entity(api=self._api,
                           fields={
                               self._field.get('f_subregion_name'): [self._origin.get('l3')],
                               self._field.get('f_subregion_region'): self.region()
                           }, bundle_id=self._bundle.get('g_subregion'))
                ]
            else:
                pass

    # Current Location
    def currentlocation(self):
        if not self._document.get('location').get('current') == []:
            self._research_data_item[self._field.get('f_research_data_item_located_at')] = self._document.get('location').get('current')
        else:
            pass

    # URL
    def url_link(self):
        if not self._document.get('url') == []:
            self._research_data_item[self._field.get('f_research_data_item_url')] = self._document.get('url')
        else:
            pass

    # Copyright
    def copyright(self):
        if not self._document.get('accessCondition')['rights'] == []:
            self._research_data_item[self._field.get('f_research_data_item_copyright')] = entity_list_generate(
                self._document.get('accessCondition')['rights'],
                self._query.get('license')
            )
        else:
            pass

    # Target Audience
    def target_audience(self):
        if not self._document.get('targetAudience') == []:
            self._research_data_item[self._field.get('f_research_data_target_audience')] = self._document.get(
                "targetAudience")
        else:
            pass

    # Abstract
    def abstract(self):
        if not self._document.get('url') == []:
            self._research_data_item[self._field.get('f_research_data_abstract')] = [self._document.get('abstract')]
        else:
            pass

    # Table of Content
    def tabel_of_content(self):
        if not self._document.get('tableOfContents') == []:
            self._research_data_item[self._field.get('f_research_data_item_toc')] = [
                self._document.get('tableOfContents')
            ]
        else:
            pass

    # Note(s)
    def note(self):
        if not self._document.get('url') == []:
            self._research_data_item[self._field.get('f_research_data_note')] = self._document.get('note')
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
        for title in self._document.get('titleInfo'):
            title_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get('f_research_data_item_title_appel'): title.get('title'),
                           self._field.get('f_research_data_item_title_type'): title.get('title_type')
                       }, bundle_id=self._bundle.get('g_research_data_item_title'))
            )
        self._research_data_item[self._bundle.get('g_research_data_item_title')] = title_entity_list

    # Dates
    # TODO: Date information section (created date to entered separately)

    # Technical Description
    # TODO: Technical Description and additional information

    # Genre
    # TODO: Genre Term by Authority (Insert authority types to system)

    # Subject
    # TODO: Subjects Values

    # Tags
    # TODO: Tags Values

    # Staged Values

    def staging(self):
        self.langauge()
        self.citation()
        self.url_link()
        self.copyright()
        self.target_audience()
        self.abstract()
        self.tabel_of_content()
        self.note()
        self.country()
        self.region()
        self.subregion()
        self.currentlocation()
        self.role()
        self.titles()
        return self._research_data_item


# Person Entity Synchronisation


class EntitySync(GeneralEntity):

    def __init__(self, mongo_auth_string, sync_field):

        # Field name initialisation
        self._sync_field_name = sync_field

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

        # MongoDB
        self._mongo_client = MongoClient(mongo_auth_string)
        self._mongo_list = self._mongo_client['dev'][self._sync_field_name].distinct('name')

        # WissKI Data
        self._wisski_entities = list(entity_uri(search_value="",
                                                query_string=self._query.get(self._wisski_query[self._sync_field_name]),
                                                return_format='csv', value_input=False).iloc[:, 0])

    def check_missing(self):
        missing_values = []
        for value in self._mongo_list:
            if value not in self._wisski_entities:
                missing_values.append(value)
        return missing_values

    def update(self):
        if not self.check_missing() == []:
            for entity in self.check_missing():
                entity_value = {
                    self._field.get(self._wisski_path_field.get(self._sync_field_name)): [entity]
                }
                entity_object = Entity(api=self._api, fields=entity_value,
                                       bundle_id=self._bundle.get(self._wisski_path_group.get(self._sync_field_name)))
                self._api.save(entity_object)