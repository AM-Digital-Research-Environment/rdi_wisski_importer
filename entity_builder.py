from datetime import datetime
from typing import NamedTuple, MutableMapping
from urllib.parse import urlparse
import re

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

    def __init__(
        self,
        api: Api,
        return_value: bool = False,
        cache: MutableMapping[str, str] = {},
    ):
        # Super class
        super().__init__()

        # API-client
        self._api = api

        # BSON Metadata Document
        self._document = None
    
        # Entity URI cache
        self._cache = cache

        # Function to return Entity values (Bool)
        self._return_value = return_value

        self._field_functions = FieldFunctions(self._api)

        # Core dictionary for Research Data Items
        self._research_data_item = {}
      
      
    def document(self, bson_document: dict):
        setattr(self, "_document", bson_document)


    # Type of Resource (Mandatory Field)
    def resource_type(self):
        _type_of_resource = [
            entity_uri(
              self._document.get('typeOfResource'),
              self._query.get('typeofresource'),
              cache=self._cache
            )
        ]
        if self._return_value:
            return _type_of_resource
        else:
            self._research_data_item[self._field.get('f_research_data_item_type_res')] = _type_of_resource


    # Project (Mandatory Field)
    def project(self):
        try:
            _project_entity = [
                entity_uri(
                  self._document.get('project')['id'],
                  self._query.get('projectid'),
                  cache=self._cache
                )
            ]
            if self._return_value:
                return _project_entity
            else:
                self._research_data_item[self._field.get('f_research_data_item_project')] = _project_entity
        except KeyError:
            pass


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
                return [collection_entity]
            else:
                # Initialising collection field
                self._research_data_item[self._bundle.get("g_res_item_collection")] = [collection_entity]


    # Entity list of identifiers
    def identifier_entities(self):
        # Initialising DRE Identifier
        _dreidfields_ = {
            self._field["f_research_data_item_id_name"]: [self._document.get("dre_id")],
            self._field["f_research_data_item_id_type"]: [
                entity_uri("DRE Identifier", self._query.get("identifier"), cache=self._cache)
            ],
        }
        _dreidentity_ = Entity(api=self._api, fields=_dreidfields_,
                               bundle_id=self._bundle['g_research_data_item_identifier'])

        entity_list = [_dreidentity_]

        # Remaining Standard Identifiers
        for iden in self._document.get('identifier'):
            identifier_fields = {
                self._field["f_research_data_item_id_name"]: [iden.get("identifier")],
                self._field["f_research_data_item_id_type"]: [
                    entity_uri(
                        iden.get("identifier_type"), self._query.get("identifier"), cache=self._cache
                    )
                ],
            }
            identifier_entity = Entity(api=self._api,
                                       fields=identifier_fields,
                                       bundle_id=self._bundle['g_research_data_item_identifier'])
            entity_list.append(identifier_entity)

        if self._return_value:
            return entity_list
        else:
            self._research_data_item[self._bundle.get('g_research_data_item_identifier')] = entity_list

    
    # Language
    def language(self):
        if self._document.get('language'):
            document_languages = [try_func(l.lower(), lambda x: self._language.get(x)) for l in self._document.get('language')]
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
        _origin = self._document.get('location').get('origin')
        _country_values = []
        _region_values = []
        _subregion_values = []

        for loc_obj in _origin:
            # Country
            if not pd.isna(loc_obj.get('l1')) and loc_obj.get('l1') != "":
                _country_values.append(
                    entity_uri(
                        search_value=loc_obj.get("l1"),
                        query_string=self._query.get("country"),
                        cache=self._cache
                    )
                )

            # Region (level 2)
            if not pd.isna(loc_obj.get('l2')) and loc_obj.get('l2') != "":
                _region_uri = entity_uri(
                    search_value=RegionFormatHolder(
                        level_0=loc_obj.get("l2"), level_1=loc_obj.get("l1")
                    ),
                    query_string=self._query.get("region"),
                    conditional=True,
                    cache=self._cache
                )
                _region_values.append(_region_uri)

            # Subregion
            """
            In case of an exception we assume, that the region values is already given.
            """
            if not pd.isna(loc_obj.get('l3')) and loc_obj.get('l3') != "":
                _subregion = entity_uri(
                    search_value=RegionFormatHolder(
                        level_0=loc_obj.get("l3"), level_1=loc_obj.get("l2")
                    ),
                    query_string=self._query.get("subregion"),
                    conditional=True,
                    cache=self._cache
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
                self._query.get('license'),
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
                self._research_data_item[self._field.get('f_research_data_item_toc')] = [self._document.get('tableOfContents')]


    # Note(s)
    def note(self):
        if self._document.get('note') and pd.isna(self._document.get('note')) is False:
            note_value = self._document.get('note')
            
            # Split into lines while preserving intentional line breaks
            lines = note_value.splitlines()
            
            # Clean each line separately
            cleaned_lines = []
            for line in lines:
                # Remove any non-printable characters while keeping extended Latin
                cleaned_line = re.sub(r'[^\x20-\x7E\u00A0-\u00FF]', '', line)
                # Normalize whitespace within each line
                cleaned_line = ' '.join(cleaned_line.strip().split())
                # Additional safety check
                cleaned_line = ''.join(char for char in cleaned_line if ord(char) >= 32)
                cleaned_lines.append(cleaned_line)
            
            # Join lines back together with newlines
            cleaned_note = '\n'.join(line for line in cleaned_lines if line)
            
            if self._return_value:
                return [cleaned_note]
            else:
                self._research_data_item[self._field.get('f_research_data_note')] = [cleaned_note]


    # Associated Person (Mandatory Field)
    def role(self):
        name_entity_list = []
        sponsor_role = entity_uri(
            search_value="Sponsor", query_string=self._query.get("role"), cache=self._cache
        )

        # Sponsor (Associated Group) (mandatory field)
        # Fetches sponsor uri/creates Entity object in case of exception
        for funder in self._document.get("sponsor"):
            sponsor_value = entity_uri(
                search_value=funder, query_string=self._query.get("sponsor"), cache=self._cache
            )
            
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
                Entity(
                    api=self._api,
                    fields={
                        self._field.get(ass_entity_type.get(qualifier)): [
                            entity_uri(
                                search_value=name.get("name").get("label"),
                                query_string=self._query.get(qualifier),
                                cache=self._cache
                            )
                        ],
                        self._field.get("f_research_data_item_apers_role"): [
                            entity_uri(
                                search_value=name.get("role"),
                                query_string=self._query.get("role"),
                                cache=self._cache
                            )
                        ],
                    },
                    bundle_id=self._bundle.get("g_research_data_item_ass_person"),
                )
            )
        if self._return_value:
            return name_entity_list
        else:
            self._research_data_item[self._bundle.get('g_research_data_item_ass_person')] = name_entity_list


    # Title Information
    def titles(self):
        title_entity_list = []
        for title in self._document.get('titleInfo'):
            if title.get('title_type') == 'main':
                _main_title = [title.get('title')]
            else:
                title_entity_list.append(
                    Entity(api=self._api,
                           fields={
                               self._field.get('f_research_data_item_title_appel'): [title.get('title')],
                               self._field.get('f_research_data_item_title_type'): [title.get('title_type')]
                           }, bundle_id=self._bundle.get('g_research_data_item_title'))
                )
        
        if self._return_value:
            return {'main': _main_title, 'alt': title_entity_list}
        else:
            self._research_data_item[self._field.get('f_research_data_item_title_main')] = _main_title
            if title_entity_list:
                self._research_data_item[self._bundle.get('g_research_data_item_title')] = title_entity_list


    # Dates
    def dateinfo(self):
        # Initialize list to store additional dates
        created_date_value = []
        additional_dates = []

        # Loop through all dates in dateInfo
        for k in self._document.get('dateInfo').keys():
            # Special handling for the created date
            if k == 'created':
                try:
                    # Convert ISO date to dd/mm/yyyy format
                    created_date_value = datetime.strftime(
                        datetime.fromisoformat(self._document.get('dateInfo').get(k).get('end')),
                        "%d/%m/%Y")
                except (TypeError, ValueError) as e:  # Catch ValueError too
                    pass
            # Handle all other date types (captured & valid dates)
            else:
                try:
                    # Create Entity objects for additional dates
                    additional_dates.append(
                        Entity(api=self._api,
                               fields={
                                   self._field.get('f_research_data_item_add_date_d'): [datetime.strftime(
                                       datetime.fromisoformat(self._document.get('dateInfo').get(k).get('end')),
                                       "%d/%m/%Y")],
                                   self._field.get('f_research_data_item_add_date_t'): [k]
                               },
                               bundle_id=self._bundle.get('g_research_data_item_date_add'))
                    )
                except (TypeError, ValueError) as e:
                    pass

        if self._return_value:
            return {'created': [created_date_value],
                    'alt': additional_dates}
        else:
            if created_date_value:
                self._research_data_item[self._field.get('f_research_data_item_create_date')] = [created_date_value]
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
            authority_uri = entity_uri(
                search_value=genre_dict.get(authority),
                query_string=self._query.get("identifier"),
                cache=self._cache
            )
            for term in genre_terms.get(authority):
                term_uri = entity_uri(
                    search_value=GenreFormatHolder(
                        term=term, authority=authority_uri.split("data/")[1]
                    ),
                    query_string=self._query.get("genre"),
                    conditional=True,
                    cache=self._cache
                )
        
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


    # Subject(s)
    def subject(self):
        if self._document.get('subject'):
            subject_list = []
            for sub in self._document.get("subject"):

                by_uri = entity_uri(sub.get("uri"), self._query.get("subjectURI"), cache=self._cache)
                by_label = entity_uri(
                    sub.get("origLabel"), self._query.get("subjectLabel"), cache=self._cache
                )

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
                        authority_uri = entity_uri(
                            sub.get("authority"),
                            query_string=self._query.get("authorityURL"),
                            cache=self._cache
                        )
                        subject_fields[self._field.get("f_subject_authority")] = [
                            authority_uri
                        ]
                    if pd.isna(sub.get("authLabel")):
                        subject_fields[self._field.get("f_subject_tag")] = [
                            sub.get("origLabel")
                        ]

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


    # Tag(s)
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


    # Preview Image URL
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

    def repository(self):
        _repo_id = re.search(r"(?<=\w{3}\-)(.*)(?=\-\w{4})", self._document.get("dre_id"))[0]
        if _repo_id != "99":
            if self._return_value:
                return [entity_uri(
                    search_value=f"R{_repo_id}",
                    query_string=self._query.get('repository'),
                    cache=self._cache
                )]
            else:
                self._research_data_item[
                    self._field.get('f_res_item_data_repository')
                ] = [entity_uri(
                    search_value=f"R{_repo_id}",
                    query_string=self._query.get('repository'),
                    cache=self._cache
                )]

    # Staged Values
    def staging(self):
        self._research_data_item = {}
        self.resource_type()
        self.project()
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
        self.repository()
        return self._research_data_item

    def upload(self):
        self._api.save(Entity(
            api=self._api,
            fields=self.staging(),
            bundle_id=self._bundle.get('g_research_data_item')
        ))
