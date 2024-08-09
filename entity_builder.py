# Data Wrangling
import pandas as pd

# Data Parsing
import re
from datetime import datetime
from urllib.parse import urlparse

# WissKi Api
from wisski.api import Api, Pathbuilder, Entity

# Local Functions
from functions import *
from auth import GeneralEntity
from exception_functions import fieldfunction


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
            self._bundle.get('g_research_data_item_identifier'): self.identifier_entities,

            # Project (Mandatory Field)
            self._field.get('f_research_data_item_project'): [
                entity_uri(self._document.get('project')['id'], self._query.get('projectid'))]
        }
    # Collection

    def collection(self):
        if not self._document.get('collection') == []:
            # Collection fields
            collection_fields = {
                self._field['f_res_item_collection']: entity_list_generate(self._document.get('collection'),
                                                                           query_name=self._query.get('collection'))}
            # Collection entity
            collection_entity = Entity(
                api=self._api, fields=collection_fields,
                bundle_id=self._bundle.get('g_res_item_collection'))
            # Initialising collection field
            self._research_data_item["g_res_item_collection"] = [collection_entity]
        else:
            pass


    # Entity list of identifiers
    @property
    def identifier_entities(self):

        # Initialising DRE Identifier
        dreId_fields = {
            self._field['f_research_data_item_id_name']: [self._document.get('dre_id')],
            self._field['f_research_data_item_id_type']: [entity_uri("DRE Identifier",
                                                                     self._query.get('identifier'))]
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
    def language(self):
        if not self._document.get('language') == []:
            document_languages = [try_func(l, lambda x: self._language.get(x)) for l in self._document.get('language')]
            self._research_data_item[self._field.get('f_research_data_item_language')] = entity_list_generate(
                document_languages,
                self._query.get('language'),
                exception_function=fieldfunction('language').exception,
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
    def originlocation(self):
        for loc_obj in self._origin:
            if not pd.isna(loc_obj.get('l1')) and loc_obj.get('l1') != "":
                self._research_data_item[self._field.get('f_research_data_creat_country')] = [
                    entity_uri(
                        search_value=loc_obj.get('l1'),
                        query_string=self._query.get('country')
                    )
                ]
            else:
                pass

    # Region (level 2)
#   def region(self):
            if not pd.isna(loc_obj.get('l2')) and loc_obj.get('l2') != "":
                region_uri = entity_uri(
                    search_value={'level_0': loc_obj.get('l2'),
                                  'level_1': self._origin.get('l1')},
                    query_string=self._query.get('region'),
                    conditional=True)
                self._research_data_item[self._field.get('f_research_data_item_creat_regio')] = [region_uri]
#                return region_uri
            else:
                pass

    # Subregion
#    def subregion(self):
        if not pd.isna(loc_obj.get('l3')) and loc_obj.get('l3') != "":
            subregion = entity_uri(
                search_value={
                    'level_0': loc_obj.get('l3'),
                    'level_1': loc_obj.get('l2')
                },
                query_string=self._query.get('subregion'),
                conditional=True
            )
            if subregion is not None and urlparse(subregion).scheme != '':
                self._research_data_item[self._field.get('f_research_data_item_creat_subre')] = [subregion]
            elif subregion is None:
                self._research_data_item[self._field.get('f_research_data_item_creat_subre')] = [
                    fieldfunction('subregion').exception(entity_value=loc_obj.get('l3'),
                                                         qualifier_value=region_uri,
                                                         with_qualifier=True)
                ]
            else:
                pass

    # Current Location
    def currentlocation(self):
        if not self._document.get('location').get('current') == []:
            self._research_data_item[
                self._field.get('f_research_data_item_located_at')
            ] = entity_list_generate(
                value_list=self._document.get('location').get('current'),
                query_name=self._query.get('place'),
                exception_function=fieldfunction('place').exception,
                with_exception=True
            )
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
                self._query.get('license'),

            )
        else:
            pass

    # Target Audience
    def target_audience(self):
        if not self._document.get('targetAudience') == []:
            self._research_data_item[self._field.get('f_research_data_target_audience')] = entity_list_generate(
                value_list=self._document.get('targetAudience'),
                query_name=self._query.get('audience'),
                exception_function=fieldfunction('audience').exception,
                with_exception=True
            )
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
    def role(self):
        name_entity_list = []
        sponsor_role = entity_uri(search_value='Sponsor', query_string=self._query.get('role'))

        # Sponsor (Associated Group) (mandatory field)
        # Fetches sponsor uri/creates Entity object in case of exception
        for funder in self._document.get('sponsor'):
            sponsor_value = entity_uri(search_value=funder, query_string=self._query.get('sponsor'))
            if sponsor_value is None:
                sponsor_value = fieldfunction('sponsor').exception(funder)
            else:
                pass
            name_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get('f_research_data_item_sponsor'): [sponsor_value],
                           self._field.get('f_research_data_item_apers_role'): [sponsor_role]
                       }, bundle_id=self._bundle.get('g_research_data_item_ass_person'))
            )
        for name in self._document.get('name'):
            name_entity_list.append(
                Entity(api=self._api,
                       fields={
                           self._field.get('f_research_data_item_role_holder'): [entity_uri(
                               search_value=name.get('name').get('label'),
                               query_string=self._query.get(name.get('name').get('qualifier'))
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
                self._research_data_item[self._field.get('f_research_data_item_title_main')] = [title.get(title)]
        else:
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

        resource_type_dict = {
        # Type (Mandatory Field)
            self._field.get('f_reseach_data_item_res_type'): [
                self._document.get('physicalDescription').get('type')
            ]
        }
        # Method
        if not pd.isna(pd_dict.get('method')):
            resource_type_dict[self._field.get('f_reseach_data_item_res_t_method')] = [pd_dict.get('method')]
        else:
            pass
        # Description
        if not len(pd_dict.get('desc')) == 0:
            resource_type_dict[self._field.get('f_reseach_data_item_res_t_desc')] = pd_dict.get('desc')
        else:
            pass
        # Technical Description Note
        if not len(pd_dict.get('note')) == 0:
            resource_type_dict[self._field.get('f_reseach_data_item_res_t_descr')] = pd_dict.get('note')
        else:
            pass

        self._research_data_item[self._bundle.get('g_reseach_data_item_res_type')] = [Entity(
            api=self._api,
            fields=resource_type_dict,
            bundle_id=self._bundle.get('g_reseach_data_item_res_type')
        )]

        # Technical Property
        if not len(pd_dict.get('tech')) == 0:
            self._research_data_item[self._field.get('f_reseach_data_item_tech_prop')] = pd_dict.get('tech')
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
            authority_uri = entity_uri(search_value=genre_dict.get(authority),
                                       query_string=self._query.get('identifier'))
            for term in genre_terms.get(authority):
                term_uri = entity_uri(search_value={'term': term, 'authority': authority_uri.split('data/')[1]},
                                      query_string=self._query.get('genre'),
                                      conditional=True)
                if term_uri is None:
                    genre_entities.append(fieldfunction('genre').exception(
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
        """
            if not len(self._document.get('subject')) == 0:
                self._research_data_item[self._field.get('f_research_data_item_subject')] = entity_list_generate(
                    value_list=self._document.get('subject'),
                    query_name=self._query.get('subject'),
                    exception_function=fieldfunction('subject').exception,
                    with_exception=True
                )
            else:
                pass
        """
        if not len(self._document.get('subject')) == 0:
            subject_list = []
            for sub in self._document.get('subject'):
                by_uri = entity_uri(sub.get('uri'), self._query.get('subjectURI'))
                by_label = entity_uri(sub.get('origLabel'), self._query.get('subjectLabel'))
                if by_uri is not None:
                    subject_list.append(by_uri)
                elif by_label is not None:
                    subject_list.append(by_label)
                else:
                    subject_fields = {}
                    if not pd.isna(sub.get('uri')):
                        subject_fields[self._field.get('f_subject_url')] = sub.get('uri')
                    else:
                        pass
                    if not pd.isna(sub.get('authority')):
                        # Authority must be in system already
                        authority_uri = entity_uri(sub.get('authority'), query_string=self._query.get('authority'))
                        subject_fields[self._field.get('f_subject_authority')] = authority_uri
                    else:
                        pass
                    if not pd.isna(sub.get('authLabel')):
                        subject_fields[self._field.get('f_subject_tag')] = sub.get('authLabel')
                    else:
                        subject_fields[self._field.get('f_subject_tag')] = sub.get('origLabel')

                    subject_list.append(
                        Entity(api=self._api, fields=subject_fields,
                               bundle_id=self._bundle.get('g_subject'))
                    )
            self._research_data_item[self._field.get('f_research_data_item_subject')] = subject_list
        else:
            pass


    # Tags
    def tags(self):
        if not len(self._document.get('tags')) == 0:
            self._research_data_item[self._field.get('f_reseach_data_item_tag')] = entity_list_generate(
                value_list=self._document.get('tags'),
                query_name=self._query.get('tags'),
                exception_function=fieldfunction('tags').exception,
                with_exception=True
            )
        else:
            pass

    # Staged Values

    def staging(self):
        self.language()
        self.citation()
        self.url_link()
        self.copyright()
        self.target_audience()
        self.abstract()
        self.tabel_of_content()
        self.note()
#        self.country()
#        self.region()
#        self.subregion()
        self.originlocation()
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
        stage = self.staging()
        self._api.save(Entity(
            api=self._api,
            fields=stage,
            bundle_id=self._bundle.get('g_research_data_item')
        ))


# Person Entity Synchronisation


class EntitySync(GeneralEntity):

    def __init__(self, mongo_auth_string: str, sync_field: str = "all"):

        # Field name initialisation
        self._mongo_auth_string = mongo_auth_string
        self._sync_field_name = sync_field
        self._bundle_name = None

        # Super Class
        super().__init__()

        # Bundle dictionary
        self._bundle_dict = {
            "persons": {
                "query": "personlist",
                "field": "f_person_name",
                "group": "g_person"
            },
            "institutions": {
                "query": "institutionlist",
                "field": "f_institution_name",
                "group": "g_institution"
            }
        }

    # MongoDB
    def mongo_list(self):
        mongo_client = MongoClient(self._mongo_auth_string)
        return mongo_client['dev'][self._bundle_name].distinct('name')

    # WissKI Data
    def wisski_entities(self):
        return list(entity_uri(search_value="",
                               query_string=self._query.get(self._bundle_dict.get(self._bundle_name)['query']),
                               return_format='csv', value_input=False).iloc[:, 0])

    # Check missing values
    def checker(self):
        missing_values = []
        for value in self.mongo_list():
            if value not in self.wisski_entities():
                missing_values.append(value)
        return missing_values

    def updator(self):
        if not self.check_missing() == []:
            for entity in self.check_missing():
                entity_value = {
                    self._field.get(self._bundle_dict.get(self._bundle_name)['field']): [entity]
                }
                entity_object = Entity(api=self._api, fields=entity_value,
                                       bundle_id=self._bundle.get(self._bundle_dict.get(self._bundle_name)['group']))
                self._api.save(entity_object)

    # Returns missing values
    def check_missing(self):
        if self._sync_field_name == 'all':
            for key in self._bundle_dict.keys():
                self._bundle_name = key
                print(self.checker())
        else:
            self._bundle_name = self._sync_field_name
            return self.checker()

    # Updates missing values
    def update(self):
        if self._sync_field_name == 'all':
            for key in self._bundle_dict.keys():
                self._bundle_name = key
                self.checker()
            return f"Updated bundle {key}"
        else:
            self._bundle_name = self._sync_field_name
            self.updator()
        return "Update Complete!"
