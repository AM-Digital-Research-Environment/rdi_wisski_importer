# Libraries
import jmespath as jp
from auth import GeneralEntity
from wisski.api import Api, Entity
from functions import entity_list_generate, entity_uri

"""
This script is intended for updating the related items sections of each research data items specified
for a given project. The class UpdateRelations will take in a Mongo collection list as arguement.
"""

class UpdateRelation(GeneralEntity):

    def __init__(self, api: Api, data: list[dict]):
        super().__init__()
        self._api = api
        self._data = data
        self._doc = []
        self._succeed_list = []
        self._precede_list = []
        self._collection_entity_dict = {}

    def set_doc(self, doc):
        setattr(
            self,
            "_doc",
            [jp.search("{id:dre_id, items:relatedItems}", doc)]
        )

    def get_entities(self, query: str | None, doc: dict):
        entities = jp.search(query, doc)
        if entities:
            return entity_list_generate(value_list=entities, query_name=self._query.get('ldID'))

    def set_entities(self, document):
        setattr(self, "_succeed_list", self.get_entities("items.rel_succ", document))
        setattr(self, "_precede_list", self.get_entities("items.rel_prec", document))

    def stage(self, document):
        self.set_entities(document=document)
        if self._succeed_list:
            self._collection_entity_dict[self._field.get('f_res_item_collection_succeeds')] = self._succeed_list
        if self._precede_list:
            self._collection_entity_dict[self._field.get('f_res_item_collection_precedes')] = self._precede_list

    def execute(self, index_value: int = None, dry_run: bool = False):

        if index_value:
            self.set_doc(self._data[index_value])
        else:
            setattr(
                self,
                "_doc",
                jp.search("[].{id:dre_id, items:relatedItems}", self._data)
            )

        for doc in self._doc:
            self.stage(doc)
            _edit_entity = self._api.get_entity(
                entity_uri(
                    doc.get('id'),
                    query_string=self._query.get('dreID')
                )
            )
            _collection_entity = Entity(
                api=self._api, fields=self._collection_entity_dict,
                bundle_id=self._bundle.get('g_res_item_collection')
            )
            _edit_entity.fields[self._bundle.get('g_res_item_collection')] = [_collection_entity]
            if dry_run:
                return _collection_entity
            else:
                self._api.save(_edit_entity)