from datetime import datetime
from typing import NamedTuple
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from wisski.api import Api, Entity

from auth import GeneralEntity
from exception_functions import FieldFunctions
from functions import entity_list_generate, entity_uri, try_func


class RegionFormatHolder(NamedTuple):
    """
    Used to format the SPARQL queries for regions.

    This is a named tuple, so that we can maintain the labels in the format
    string, and have a hashable object that `functools.cache` can use as cache
    keys.
    """
    level_0: str
    level_1: str


class GenreFormatHolder(NamedTuple):
    """
    Used to format the SPARQL queries for genres.

    This is a named tuple, so that we can maintain the labels in the format
    string, and have a hashable object that `functools.cache` can use as cache
    keys.
    """
    term: str
    authority: str


class DocumentEntity(GeneralEntity):
    """
    Class for creating the Research Data Item entity
    """

    def __init__(self, bson_doc, api: Api, return_value: bool = False):

        # Super class
        super().__init__()

        # API-client
        self._api = api

        # BSON Metadata Document
        self._document = bson_doc

        # Function to return Entity values (Bool)
        self._return_value = return_value

        # GeoLoc Information
        self._origin = self._document.get('location').get('origin')

        self._field_functions = FieldFunctions(self._api)

        # Core dictionary for Research Data Items
        self._research_data_item = {

            # Type of Resource (Mandatory Field)
            self._field.get('f_research_data_item_type_res'): [
                entity_uri(self._document.get('typeOfResource'), self._query.get('typeofresource'))],

            # Field for Identifiers (Mandatory Field)
            self._bundle.get('g_research_data_item_identifier'): self.identifier_entities(),

            # Project (Mandatory Field)
            self._field.get('f_research_data_item_project'): [
                entity_uri(self._document.get('project')['id'], self._query.get('projectid'))]
        }
    # Collection

    def collection(self):
        if self._document.get('collection'):
            # Collection fields
            collection_fields = {
                self._field['f_res_item_collection']: entity_list_generate(self._document.get('collection'),
                                                                           query_name=self._query.get('collection'))}
            # Collection entity
            collection_entity = Entity(
                api=self._api, fields=collection_fields,
                bundle_id=self._bundle.get('g_res_item_collection'))
            if self._return_value:
                return collection_entity
            else:
                # Initialising collection field
                self._research_data_item[self._bundle.get("g_res_item_collection")] = [collection_entity]

    # Entity list of identifiers

    def identifier_entities(self):
        # Initialising DRE Identifier
        _dreidfields_ = {
            self._field['f_research_data_item_id_name']: [self._document.get('dre_id')],
            self._field['f_research_data_item_id_type']: [entity_uri("DRE Identifier",
                                                                     self._query.get('identifier'))]
        }
        _dreidentity_ = Entity(api=self._api, fields=_dreidfields_,
                               bundle_id=self._bundle['g_research_data_item_identifier'])

        entity_list = [_dreidentity_]

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
    def language(self):
        if self._document.get('language'):
            document_languages = [try_func(l, lambda x: self._language.get(x)) for l in self._document.get('language')]
            lang_list = entity_list_generate(
                document_languages,
                self._query.get('language'),
                exception_function=self._field_functions.exception('language'),
                with_exception=True
            )
            if self._return_value:
                return lang_list
            else:
                self._research_data_item[self._field.get('f_research_data_item_language')] = lang_list

    # Citation
    def citation(self):
        if self._document.get('citation'):
            citation_value = self._document.get('citation')
            if self._return_value:
                return citation_value
            else:
                self._research_data_item[
                    self._field.get('f_research_data_item_citation')
                ] = self._document.get('citation')

    # Geographic Location
    def originlocation(self):

        _country_values = []
        _region_values = []
        _subregion_values = []

        for loc_obj in self._origin:

            # Country
            if not pd.isna(loc_obj.get('l1')) and loc_obj.get('l1') != "":
                _country_values.append(
                    entity_uri(
                        search_value=loc_obj.get('l1'),
                        query_string=self._query.get('country')
                    )
                )

            # Region (level 2)
            if not pd.isna(loc_obj.get('l2')) and loc_obj.get('l2') != "":
                _region_uri = entity_uri(
                    search_value=RegionFormatHolder(level_0=loc_obj.get('l2'), level_1=loc_obj.get('l1')),
                    query_string=self._query.get('region'),
                    conditional=True)
                _region_values.append(_region_uri)

            # Subregion
            """
            In case of an exception we assume, that the region values is already given.
            """
            if not pd.isna(loc_obj.get('l3')) and loc_obj.get('l3') != "":
                _subregion = entity_uri(
                    search_value=RegionFormatHolder(level_0=loc_obj.get('l3'), level_1=loc_obj.get('l2')),
                    query_string=self._query.get('subregion'),
                    conditional=True
                )
                if _subregion and urlparse(_subregion).scheme != '':
                    _subregion_values.append(_subregion)
                elif not _subregion:
                    _subregion_values.append(
                        self._field_functions.exception('subregion')(entity_value=loc_obj.get('l3'),
                                                                     qualifier_value=_region_uri,
                                                                     with_qualifier=True)
                    )
        if self._return_value:
            _ol_return_dicts = {}
            if _country_values:
                _ol_return_dicts['l1'] = _country_values
            if _region_values:
                _ol_return_dicts['l2'] = _region_values
            if _subregion_values:
                _ol_return_dicts['l3'] = _subregion_values
            return _ol_return_dicts
        else:
            if _country_values:
                self._research_data_item[self._field.get('f_research_data_creat_country')] = _country_values
            if _region_values:
                self._research_data_item[self._field.get('f_research_data_item_creat_regio')] = _region_values
            if _subregion_values:
                self._research_data_item[self._field.get('f_research_data_item_creat_subre')] = _subregion_values

    # Current Location
    def currentlocation(self):
        if self._document.get('location').get('current'):
            _current_location_value = entity_list_generate(
                value_list=self._document.get('location').get('current'),
                query_name=self._query.get('place'),
                exception_function=self._field_functions.exception('place'),
                with_exception=True
            )
            if self._return_value:
                return _current_location_value
            else:
                self._research_data_item[
                    self._field.get('f_research_data_item_located_at')
                ] = _current_location_value

    # URL
    def url_link(self):
        if self._document.get('url'):
            if self._return_value:
                return self._document.get('url')
            else:
                self._research_data_item[self._field.get('f_research_data_item_url')] = self._document.get('url')

    # Copyright
    def copyright(self):
        if self._document.get('accessCondition')['rights']:
            _copyright_values = entity_list_generate(
                self._document.get('accessCondition')['rights'],
                self._query.get('license')
            )
            if self._return_value:
                return _copyright_values
            else:
                self._research_data_item[self._field.get('f_research_data_item_copyright')] = _copyright_values

    # Target Audience
    def target_audience(self):
        if self._document.get('targetAudience'):
            _target_audience_values = entity_list_generate(
                value_list=self._document.get('targetAudience'),
                query_name=self._query.get('audience'),
                exception_function=self._field_functions.exception('audience'),
                with_exception=True
            )
            if self._return_value:
                return _target_audience_values
            else:
                self._research_data_item[self._field.get('f_research_data_target_audience')] = _target_audience_values

    # Abstract
    def abstract(self):
        if self._document.get('abstract') and pd.isna(self._document.get('abstract')) is False:
            if self._return_value:
                return [self._document.get('abstract')]
            else:
                self._research_data_item[self._field.get('f_research_data_abstract')] = [self._document.get('abstract')]

    # Table of Content
    def tabel_of_content(self):
        if self._document.get('tableOfContents') and pd.isna(self._document.get('tableOfContents')) is False:
            if self._return_value:
                return [self._document.get('tableOfContents')]
            else:
                self._research_data_item[self._field.get('f_research_data_item_toc')] = [
                    self._document.get('tableOfContents')
                ]

    # Note(s)
    def note(self):
        if self._document.get('note') and pd.isna(self._document.get('note')) is False:
            if self._return_value:
                return self._document.get('note')
            else:
                self._research_data_item[self._field.get('f_research_data_note')] = self._document.get('note')

    # Associated Person (Mandatory Field)
    def role(self):
        name_entity_list = []
        sponsor_role = entity_uri(search_value='Sponsor', query_string=self._query.get('role'))

        # Sponsor (Associated Group) (mandatory field)
        # Fetches sponsor uri/creates Entity object in case of exception
        for funder in self._document.get('sponsor'):
            sponsor_value = entity_uri(search_value=funder, query_string=self._query.get('sponsor'))
            if sponsor_value is None:
                sponsor_value = self._field_functions.exception('sponsor')(funder)
            name_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get('f_research_data_item_sponsor'): [sponsor_value],
                           self._field.get('f_research_data_item_apers_role'): [sponsor_role]
                       }, bundle_id=self._bundle.get('g_research_data_item_ass_person'))
            )
        for name in self._document.get('name'):
            qualifier = name.get('name').get('qualifier')
            ass_entity_type = {
                'person': 'f_research_data_item_role_holder',
                'group': 'f_research_data_item_role_hldr_g',
                'institution': 'f_research_data_item_role_hldr_i'
            }
            name_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get(ass_entity_type.get(qualifier)): [entity_uri(
                               search_value=name.get('name').get('label'),
                               query_string=self._query.get(qualifier)
                           )],
                           self._field.get('f_research_data_item_apers_role'): [entity_uri(
                               search_value=name.get('role'),
                               query_string=self._query.get('role')
                           )]
                       }, bundle_id=self._bundle.get('g_research_data_item_ass_person'))
            )
        self._research_data_item[self._bundle.get('g_research_data_item_ass_person')] = name_entity_list

    # Title Information
    def titles(self):
        title_entity_list = []
        for title in self._document.get('titleInfo'):
            if title.get('title_type') == 'main':
                self._research_data_item[self._field.get('f_research_data_item_title_main')] = [title.get('title')]
            else:
                title_entity_list.append(
                    Entity(api=self._api,
                           fields={
                               self._field.get('f_research_data_item_title_appel'): [title.get('title')],
                               self._field.get('f_research_data_item_title_type'): [title.get('title_type')]
                           }, bundle_id=self._bundle.get('g_research_data_item_title'))
                )
        if title_entity_list:
            if self._return_value:
                return title_entity_list
            else:
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
        if additional_dates:
            self._research_data_item[self._bundle.get('g_research_data_item_date_add')] = additional_dates

    # Technical Description

    def physicaldesc(self):
        pd_dict = self._document.get('physicalDescription')

        resource_type_dict = {
            # Type (Mandatory Field)
            self._field.get('f_reseach_data_item_res_type'): [
                self._document.get('physicalDescription').get('type')
            ]
        }
        # Method
        if not pd.isna(pd_dict.get('method')):
            resource_type_dict[self._field.get('f_reseach_data_item_res_t_method')] = [pd_dict.get('method')]
        # Description
        if pd_dict.get('desc'):
            resource_type_dict[self._field.get('f_reseach_data_item_res_t_desc')] = pd_dict.get('desc')
        # Technical Description Note
        if pd_dict.get('note'):
            resource_type_dict[self._field.get('f_reseach_data_item_res_t_descr')] = pd_dict.get('note')

        pd_entity = [Entity(
            api=self._api,
            fields=resource_type_dict,
            bundle_id=self._bundle.get('g_reseach_data_item_res_type')
        )]

        if self._return_value:
            return pd_entity
        else:
            self._research_data_item[self._bundle.get('g_reseach_data_item_res_type')] = pd_entity

        # Technical Property
        if pd_dict.get('tech'):
            self._research_data_item[self._field.get('f_reseach_data_item_tech_prop')] = pd_dict.get('tech')

    # Genre

    def genre(self):
        genre_dict = {
            'marc': 'MARC Genre Term List',
            'loc': 'Library of Congress Genre',
            'aat': 'Art & architecture thesaurus online',
            'tgm2': 'Thesaurus For Graphic Materials',
            'none': 'No Authority/Uncatalogued Genre'
        }
        genre_entities = []
        genre_terms = self._document.get('genre')
        for authority in genre_terms.keys():
            authority_uri = entity_uri(search_value=genre_dict.get(authority),
                                       query_string=self._query.get('identifier'))
            for term in genre_terms.get(authority):
                term_uri = entity_uri(
                    search_value=GenreFormatHolder(term=term, authority=authority_uri.split('data/')[1]),
                    query_string=self._query.get('genre'),
                    conditional=True)
                if term_uri is None:
                    genre_entities.append(self._field_functions.exception('genre')(
                        entity_value=term,
                        qualifier_value=authority_uri,
                        with_qualifier=True
                    ))
                elif urlparse(term_uri).scheme != '':
                    genre_entities.append(term_uri)

            if self._return_value:
                return genre_entities
            else:
                self._research_data_item[self._field.get('f_research_data_item_auth_tag')] = genre_entities

    # Subject
    def subject(self):
        if self._document.get('subject'):
            subject_list = []
            for sub in self._document.get('subject'):
                by_uri = entity_uri(sub.get('uri'), self._query.get('subjectURI'))
                by_label = entity_uri(sub.get('origLabel'), self._query.get('subjectLabel'))
                if by_uri:
                    subject_list.append(by_uri)
                elif by_label:
                    subject_list.append(by_label)
                else:
                    subject_fields = {}
                    if not pd.isna(sub.get('uri')):
                        subject_fields[self._field.get('f_subject_url')] = [sub.get('uri')]
                    if not pd.isna(sub.get('authority')):
                        # Authority must be in system already
                        authority_uri = entity_uri(sub.get('authority'), query_string=self._query.get('authorityURL'))
                        subject_fields[self._field.get('f_subject_authority')] = [authority_uri]
                    if pd.isna(sub.get('authLabel')):
                        subject_fields[self._field.get('f_subject_tag')] = [sub.get('origLabel')]
                    else:
                        subject_fields[self._field.get('f_subject_tag')] = [sub.get('authLabel')]

                    subject_list.append(
                        Entity(api=self._api, fields=subject_fields,
                               bundle_id=self._bundle.get('g_subject'))
                    )
            if self._return_value:
                return subject_list
            else:
                self._research_data_item[self._field.get('f_research_data_item_subject')] = subject_list

    # Tags
    def tags(self):
        if self._document.get('tags'):
            _tag_values = entity_list_generate(
                value_list=self._document.get('tags'),
                query_name=self._query.get('tags'),
                exception_function=self._field_functions.exception('tags'),
                with_exception=True
            )
            if self._return_value:
                return _tag_values
            else:
                self._research_data_item[self._field.get('f_reseach_data_item_tag')] = _tag_values

    # URL
    def preview_image(self):
        try:
            if self._document.get('previewImage'):
                if self._return_value:
                    return self._document.get('previewImage')
                else:
                    self._research_data_item[
                        self._field.get('f_research_data_item_prv_img_url')
                    ] = self._document.get('previewImage')
        except KeyError:
            pass

    # Staged Values

    def staging(self):
        self.collection()
        self.identifier_entities()
        self.language()
        self.citation()
        self.originlocation()
        self.currentlocation()
        self.url_link()
        self.copyright()
        self.target_audience()
        self.abstract()
        self.tabel_of_content()
        self.note()
        self.role()
        self.titles()
        self.dateinfo()
        self.physicaldesc()
        self.genre()
        self.subject()
        self.tags()
        self.preview_image()
        return self._research_data_item

    def upload(self):
        stage = self.staging()
        self._api.save(Entity(
            api=self._api,
            fields=stage,
            bundle_id=self._bundle.get('g_research_data_item')
        ))
