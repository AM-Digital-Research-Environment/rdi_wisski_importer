# Libraries
from entity_builder import DocumentEntity
from wisski.api import Api, Entity
from functions import entity_uri
from types import MethodType
from pymongo import cursor

# Summary
"""
This class file holds the class used for updating pre-existing WissKI enities
by specified field. The reference to each entity will be made using
DRE Identifier (all fields can be updated, with the exception of identifiers)
"""

# Pseudo-Code
"""
Input: DRE Identifier (string), field name (string)
Process:
The class wil use the update the specified field.
Ouput: Success report
"""


class DocumentUpdate(DocumentEntity):

    def __init__(self, api: Api,
                 method: str | list[str],
                 mongo_data: list):

        # Local initialisation
        self._api = api
        self._bson_doc_list = mongo_data
        self._method = method if isinstance(method, list) else [method]
        self._edit_entity = None

        # Super class initialisation
        super().__init__(api=self._api, return_value=True)

    # Method match case method
    def run(self, value_append: bool = False, new_value=None, dry_run=False):

        for doc in self._bson_doc_list:

            # Passing bson document
            self.document(bson_document=doc)

            # Initialising Entity to be edited
            setattr(self,
                    "_edit_entity",
                    self._api.get_entity(
                        entity_uri(
                            search_value=doc.get('dre_id'),
                            query_string=self._query.get('dreID')
                        )
                    )
                    )

            for _method_value in self._method:

                kwargs = {
                    "doc_id": doc.get('dre_id'),
                    "method": _method_value,
                    "dry_run": dry_run,
                    "push_new_value": value_append,
                    "push_value": new_value
                    }

                match _method_value:

                    case 'collection':
                        self.build(
                            **kwargs,
                            field_name=self._bundle.get("g_res_item_collection"),
                            default_values=self.collection()
                        )
                    
                    case 'project':
                        self.build(
                            **kwargs,
                            field_name=self._field.get("f_research_data_item_project"),
                            default_values=self.project()
                        )

                    case 'AssociatedEntities':
                        self.build(
                            **kwargs,
                            field_name=self._bundle.get('g_research_data_item_ass_person'),
                            default_values=self.role()
                        )

                    case 'language':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_item_language'),
                            default_values=self.language()
                        )

                    case 'citation':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_item_citation'),
                            default_values=self.citation()
                            )

                    case 'country':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_creat_country'),
                            default_values=self.originlocation().get('l1')
                            )

                    case 'region':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_creat_regio'),
                            default_values=self.originlocation().get('l2')
                            )

                    case 'subregion':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_creat_subre'),
                            default_values=self.originlocation().get('l3')
                            )

                    case 'currentLocation':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_item_located_at'),
                            default_values=self.currentlocation()
                            )

                    case 'physicalDesc':
                        self.build(
                            **kwargs,
                            field_name=self._bundle.get('g_reseach_data_item_res_type'),
                            default_values=self.physicaldesc()
                            )

                    case 'note':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_note'),
                            default_values=self.note()
                            )
                    
                    case 'abstract':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_abstract'),
                            default_values=self.abstract()
                            )
                    
                    case 'tags':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_reseach_data_item_tag'),
                            default_values=self.tags()
                            )
                    
                    case 'targetAudience':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_target_audience'),
                            default_values=self.target_audience()
                            )
                    
                    case 'url':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_item_url'),
                            default_values=self.url_link()
                            )
                    
                    case 'mainTitle':
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_item_title_main'),
                            default_values=self.titles()['main']
                        )
                    
                    case 'altTitle':                            
                        self.build(
                            **kwargs,
                            field_name=self._field.get('g_research_data_item_title'),
                            default_values=self.titles()['alt']
                        )
                    
                    case 'createDate':                            
                        self.build(
                            **kwargs,
                            field_name=self._field.get('f_research_data_item_create_date'),
                            default_values=self.dateinfo()['created']
                        )
                    
                    case 'altDate':                            
                        self.build(
                            **kwargs,
                            field_name=self._bundle.get('g_research_data_item_date_add'),
                            default_values=self.dateinfo()['alt']
                        )
                    
                    case _:
                        print(f'No field found with name {_method_value}')

        if not dry_run:
            print("All updates completed.")

    def build(self,
              doc_id: str,
              method: str,
              dry_run: bool = False,
              push_new_value: bool = False,
              push_value=None,
              field_name=None,
              default_values: MethodType = None) -> str:
        if dry_run:
            print("Dry run initiated, preview values given below:\n")
            print(default_values)
        else:
            print(f"Updating fields for the DRE ID: {doc_id}")
            try:
                if push_new_value:
                    self._edit_entity.fields[field_name] = self._edit_entity.fields[field_name].append(push_value)
                    self._api.save(self._edit_entity)
                else:
                    if default_values == None:
                        self._edit_entity.fields[field_name] = []
                        self._api.save(self._edit_entity)
                    else:
                        self._edit_entity.fields[field_name] = default_values
                        self._api.save(self._edit_entity)          
                print("{field} updated!".format(field=method))
            except KeyError:
                print("{field} was not updated. Please check values passed.".format(field=method))
