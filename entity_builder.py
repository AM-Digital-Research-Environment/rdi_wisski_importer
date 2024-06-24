# Libraries
import numpy as np
# MongoDB
from pymongo import MongoClient

# Data Wrangling
import pandas as pd

# Data Parsing
import json
from datetime import datetime
from urllib.parse import urlparse

# WissKi Api
from wisski.api import Api, Pathbuilder, Entity

# Local Functions
import functions
from auth import GeneralEntity
from exception_functions import fieldfunction


# Class for creating the Research Data Item entity

class DocumentEntity(GeneralEntity):

    def __init__(self, bson_doc, api=None, known_entities=None):

        # Super class
        super().__init__(api, known_entities)

        # BSON Metadata Document
        self._document = bson_doc
        self.api = api
        self.known_entities = known_entities

        # GeoLoc Information
        self._origin = self._document.get('location').get('origin')

        # Core dictionary for Research Data Items
        self._research_data_item = {

            # Type of Resource (Mandatory Field)
            self._field.get('f_research_data_item_type_res'): [
                self.entity_uri(self._document.get('typeOfResource'), 'typeofresource')],

            # Field for Identifiers (Mandatory Field)
            self._bundle.get('g_research_data_item_identifier'): self.identifier_entities(),

            # Project (Mandatory Field)
            self._field.get('f_research_data_item_project'): [
                self.entity_uri(self._document.get('project')['id'], 'projectid')]
        }
    # Entity list of identifiers

    def identifier_entities(self):

        # Initialising DRE Identifier
        dreId_fields = {
            self._field['f_research_data_item_id_name']: [self._document.get('dre_id')],
            self._field['f_research_data_item_id_type']: [self.entity_uri("DRE Identifier", 'identifier')]
        }
        
        dreId_entity = Entity(api=self._api, fields=dreId_fields,
                              bundle_id=self._bundle['g_research_data_item_identifier'])

        entity_list = [dreId_entity]

        # Remaining Standard Identifiers
        for iden in self._document.get('identifier'):
            identifier_fields = {
                self._field['f_research_data_item_id_name']: [iden.get('identifier')],
                self._field['f_research_data_item_id_type']: [
                    self.entity_uri(iden.get('identifier_type'), 'identifier')]
            }
            identifier_entity = Entity(api=self._api,
                                       fields=identifier_fields,
                                       bundle_id=self._bundle['g_research_data_item_identifier'])
            entity_list.append(identifier_entity)
        return entity_list

    # Language
    def langauge(self):
        if not self._document.get('langauge') == []:
            document_languages = [try_func(l, lambda x: self._language.get(x)) for l in self._document.get('language')]
            self._research_data_item[self._field.get('f_research_data_item_language')] = self.entity_list_generate(
                document_languages,
                'language',
                exception_function=fieldfunction('language', self.api, self.known_entities).exception,
                with_exception=True
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
            self._research_data_item[self._field.get('f_research_data_creat_country')] = [
                self.entity_uri(
                    search_value=self._origin.get('l1'),
                    query_id='country'
                )
            ]
        else:
            pass

    # Region (level 2)
    def region(self):
        if not pd.isna(self._origin.get('l2')):
            region_uri = self.entity_uri(
                search_value={'level_0': self._origin.get('l2'),
                              'level_1': self._origin.get('l1')},
                query_string='region',
                conditional=True)
            self._research_data_item[self._field.get('f_research_data_item_creat_regio')] = [region_uri]
            return region_uri
        else:
            pass

    # Subregion
    def subregion(self):
        if not pd.isna(self._origin.get('l3')):
            subregion = self.entity_uri(
                search_value={
                    'level_0': self._origin.get('l3'),
                    'level_1': self._origin.get('l2')
                },
                query_string='subregion',
                conditional=True
            )
            if subregion is not None and urlparse(subregion).scheme != '':
                self._research_data_item[self._field.get('f_research_data_item_creat_subre')] = [subregion]
            elif subregion is None:
                self._research_data_item[self._field.get('f_research_data_item_creat_subre')] = [
                    fieldfunction('subregion', self.api, self.known_entities).exception(entity_value=self._origin.get('l3'),
                                                         qualifier_value=self.region(),
                                                         with_qualifier=True)
                ]
            else:
                pass

    # Current Location
    def currentlocation(self):
        if not self._document.get('location').get('current') == []:
            self._research_data_item[
                self._field.get('f_research_data_item_located_at')
            ] = self._document.get('location').get('current')
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
            self._research_data_item[self._field.get('f_research_data_item_copyright')] = self.entity_list_generate(
                self._document.get('accessCondition')['rights'],
                'license'
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
        if not self._document.get('abstract') == [] and pd.isna(self._document.get('abstract')) is False:
            self._research_data_item[self._field.get('f_research_data_abstract')] = [self._document.get('abstract')]
        else:
            pass

    # Table of Content
    def tabel_of_content(self):
        if not self._document.get('tableOfContents') == [] and pd.isna(self._document.get('tableOfContents')) is False:
            self._research_data_item[self._field.get('f_research_data_item_toc')] = [
                self._document.get('tableOfContents')
            ]
        else:
            pass

    # Note(s)
    def note(self):
        if not self._document.get('note') == [] and pd.isna(self._document.get('note')) is False:
            self._research_data_item[self._field.get('f_research_data_note')] = [self._document.get('note')]
        else:
            pass

    # Associated Person (Mandatory Field)
    # TODO: Sort out pathbuilder config for this bundle
    def role(self):
        name_entity_list = []

        # Sponsor (Associated Group) (mandatory field)
        for funder in self._document.get('sponsor'):
            name_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get('f_research_data_item_sponsor'): [self.entity_uri(
                               search_value=funder,
                               query_string='sponsor'
                           )],
                           self._field.get('f_research_data_item_apers_role'): [self.entity_uri(
                               search_value='Sponsor',
                               query_string='role'
                           )]
                       }, bundle_id=self._bundle.get('g_research_data_item_ass_person'))
            )

        for name in self._document.get('name'):
            name_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get('f_research_data_item_role_holder'): [self.entity_uri(
                               search_value=name.get('name'),
                               query_string='person'
                           )],
                           self._field.get('f_research_data_item_apers_role'): [self.entity_uri(
                               search_value=name.get('role'),
                               query_string='role'
                           )]
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
                           self._field.get('f_research_data_item_title_appel'): [title.get('title')],
                           self._field.get('f_research_data_item_title_type'): [title.get('title_type')]
                       }, bundle_id=self._bundle.get('g_research_data_item_title'))
            )
        self._research_data_item[self._bundle.get('g_research_data_item_title')] = title_entity_list

    # Dates
    def dateinfo(self):
        additional_dates = []
        for k in self._document.get('dateInfo').keys():
            if k == 'created':
                try:
                    date_value = datetime.strftime(
                        datetime.fromisoformat(self._document.get('dateInfo').get(k).get('end')),
                        "%d/%m/%Y")
                    self._research_data_item[self._field.get('f_research_data_item_create_date')] = [date_value]
                except TypeError:
                    pass
            else:
                try:
                    additional_dates.append(
                        Entity(api=self._api,
                               fields={
                                   self._field.get('f_research_data_item_add_date_d'): datetime.strftime(
                                       datetime.fromisoformat(self._document.get('dateInfo').get(k).get('end')),
                                       "%d/%m/%Y"),
                                   self._field.get('f_research_data_item_add_date_t'): k
                               },
                               bundle_id=self._bundle.get('g_research_data_item_date_add'))
                    )
                except TypeError:
                    pass
        if additional_dates != []:
            self._research_data_item[self._bundle.get('g_research_data_item_date_add')] = additional_dates
        else:
            pass

    # Technical Description

    def physicaldesc(self):
        pd_dict = self._document.get('physicalDescription')
        # Type (Mandatory Field)
        self._research_data_item[self._field.get('f_reseach_data_item_res_type')] = [
            self._document.get('physicalDescription').get('type')
        ]
        # Method
        if not pd.isna(pd_dict.get('method')):
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_method')] = [pd_dict.get('method')]
        else:
            pass
        # Description
        if not len(pd_dict.get('desc')) == 0:
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_desc')] = pd_dict.get('desc')
        else:
            pass
        # Technical Property
        if not len(pd_dict.get('tech')) == 0:
            self._research_data_item[self._field.get('f_reseach_data_item_tech_prop')] = pd_dict.get('tech')
        else:
            pass
        # Technical Description Note
        if not len(pd_dict.get('note')) == 0:
            self._research_data_item[self._field.get('f_reseach_data_item_res_t_descr')] = pd_dict.get('note')
        else:
            pass

    # Genre

    def genre(self):
        genre_dict = {
            'marc': 'Machine-Readable Cataloging',
            'loc': 'LC Genre',
            'aat': 'Art & architecture thesaurus online',
            'tgm2': 'Thesaurus For Graphic Materials',
            'none': 'No Authority/Uncatalogued Genre'
        }
        genre_entities = []
        genre_terms = self._document.get('genre')
        for authority in genre_terms.keys():
            authority_uri = self.entity_uri(search_value=genre_dict.get(authority),
                                       query_id='identifier')
            for term in genre_terms.get(authority):
                term_uri = self.entity_uri(search_value={'term': term, 'authority': authority_uri.split('data/')[1]},
                                      query_id='genre',
                                      conditional=True)
                if term_uri is None:
                    genre_entities.append(fieldfunction('genre', self.api, self.known_entities).exception(
                        entity_value=term,
                        qualifier_value=authority_uri,
                        with_qualifier=True
                    ))
                elif urlparse(term_uri).scheme != '':
                    genre_entities.append(term_uri)
                else:
                    pass
            self._research_data_item[self._field.get('f_research_data_item_auth_tag')] = genre_entities

    # Subject
    def subject(self):
        if not len(self._document.get('subject')) == 0:
            self._research_data_item[self._field.get('f_research_data_item_subject')] = entity_list_generate(
                value_list=self._document.get('subject'),
                query_id='subject',
                exception_function=fieldfunction('subject', self.api, self.known_entities).exception,
                with_exception=True
            )
        else:
            pass

    # Tags
    # TODO: Tags Values
    def tags(self):
        if not len(self._document.get('tags')) == 0:
            self._research_data_item[self._field.get('f_reseach_data_item_tag')] = entity_list_generate(
                value_list=self._document.get('tags'),
                query_id='tags',
                exception_function=fieldfunction('tags', self.api, self.known_entities).exception,
                with_exception=True
            )
        else:
            pass

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
        self.dateinfo()
        self.genre()
        self.subject()
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
        self._wisski_entities = list(functions.entity_uri(search_value="",
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
