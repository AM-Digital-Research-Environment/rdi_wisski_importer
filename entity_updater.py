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

# Libraries
from entity_builder import DocumentEntity
from wisski.api import Api, Entity
from functions import entity_uri
from types import MethodType
from pymongo import cursor

class DocumentUpdate(DocumentEntity):

    def __init__(self, api: Api,
                 method: str | list[str],
                 mongo_data: cursor):

        # Local initialisation
        self._api = api
        self._bson_doc_list = list(mongo_data)
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

            if not dry_run:
                print(f"Updating fields for the DRE ID: {doc.get('dre_id')}")

            for _method_value in self._method:

                match _method_value:

                    case 'collection':
                        if dry_run:
                            print(self.collection())
                        else:
                            print(self.build(
                                push_new_value=value_append,
                                push_value=new_value,
                                field_name=self._bundle.get("g_res_item_collection"),
                                default_values=self.collection()
                            ).format(field=_method_value))
                    # Todo: Identifier field case to be added

                    case 'langauge':
                        if dry_run:
                            print(self.language())
                        else:
                            print(self.build(
                                push_new_value=value_append,
                                push_value=new_value,
                                field_name=self._field.get('f_research_data_item_language'),
                                default_values=self.language()
                            ).format(field=_method_value))

                    case 'citation':
                        if dry_run:
                            print(self.citation())
                        else:
                            print(self.build(
                                push_new_value=value_append,
                                push_value=new_value,
                                field_name=self._field.get('f_research_data_item_citation'),
                                default_values=self.citation()
                            ).format(field=_method_value))

                    case 'country':
                        if dry_run:
                            print(self.originlocation().get('l1'))
                        else:
                            print(self.build(
                                 push_new_value=value_append,
                                 push_value=new_value,
                                 field_name=self._field.get('f_research_data_creat_country'),
                                 default_values=self.originlocation().get('l1')
                            ).format(field=_method_value))

                    case 'region':
                        if dry_run:
                            print(self.originlocation().get('l2'))
                        else:
                            print(self.build(
                                push_new_value=value_append,
                                push_value=new_value,
                                field_name=self._field.get('f_research_data_creat_regio'),
                                default_values=self.originlocation().get('l2')
                            ).format(field=_method_value))

                    case 'subregion':
                        if dry_run:
                            print(self.originlocation().get('l3'))
                        else:
                            print(self.build(
                                push_new_value=value_append,
                                push_value=new_value,
                                field_name=self._field.get('f_research_data_creat_subre'),
                                default_values=self.originlocation().get('l3')
                            ).format(field=_method_value))

                    case 'currentLocation':
                        if dry_run:
                            print(self.currentlocation())
                        else:
                            print(self.build(
                                push_new_value=value_append,
                                push_value=new_value,
                                field_name=self._field.get('f_research_data_item_located_at'),
                                default_values=self.currentlocation()
                            ).format(field=_method_value))

                    case 'physicalDesc':
                        if dry_run:
                            print(self.physicaldesc())
                        else:
                            print(self.build(
                                push_new_value=value_append,
                                push_value=new_value,
                                field_name=self._bundle.get('g_reseach_data_item_res_type'),
                                default_values=self.physicaldesc()
                            ).format(field=_method_value))

                    case 'note':
                        if dry_run:
                            print(self.note())
                        else:
                            print(self.build(
                                push_new_value=value_append,
                                push_value=new_value,
                                field_name=self._field.get('f_research_data_note'),
                                default_values=self.note()
                            ).format(field=_method_value))

                    case _:
                        print(f'No field found with name {_method_value}')

                print("All updates completed.")

    def build(self,
              push_new_value: bool = False,
              push_value=None,
              field_name=None,
              default_values: MethodType = None) -> str:
        try:
            if push_new_value:
                self._edit_entity.fields[field_name] = self._edit_entity.fields[field_name].append(push_value)
            else:
                self._edit_entity.fields[field_name] = default_values
            self._api.save(self._edit_entity)
            return "{field} updated!"
        except:
            return "{field} was not updated. Please check values passed."
