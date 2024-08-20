# Data Parsing
import jmespath as jm

# Class for Associated Entities Synchronisation

class EntitySync(GeneralEntity):
    """
    PSEUDO-CODE

    - List of mongo entities
    - List of WissKI bundle entities
    - Compare function
    - Check function/ Staged function
    - Update function

    """


    def __init__(self, mongo_auth_string: str, sync_field: str = "all"):

        # Field name initialisation
        self._mongo_auth_string = mongo_auth_string
        self._sync_field_name = sync_field
        self._mongo_client = MongoClient(self._mongo_auth_string)
        self._bundle_name = None
        self._single_fields = ['institutions', 'groups']
        self._double_fields = ['persons', 'collections']
        self._double_fields_holder = []
        self._staged_values = []

        # Super Class
        super().__init__()

        # Bundle dictionary
        self._bundle_dict = {
            "institutions": {
                "query": "institutionlist",
                "field": "f_institution_name",
                "group": "g_institution"
            },
            "persons": {
                "query": "personlist",
                "field": "f_person_name",
                "alt_field": "f_person_affiliation",
                "alt_field_label": "affiliation",
                "group": "g_person"
            },
            "groups": {
                "query": "grouplist",
                "field": "f_group_name",
                "group": "g_group"
            },
            "collections": {
                "query": "collectionlist",
                "field": "f_collection_title",
                "alt_field": "f_collection_identifier",
                "alt_field_label": "identifier",
                "group": "g_collection"
            }
        }

    # MongoDB
    def mongo_list(self):
        if self._sync_field_name in self._single_fields:
            return self._mongo_client['dev'][self._bundle_name].distinct('name')
        elif self._sync_field_name in self._double_fields:
            names_list = self._mongo_client['dev'][self._bundle_name].distinct('name')
            self._double_fields_holder = list(self._mongo_client['dev'][self._bundle_name].find())
            return names_list

    # WissKI Data
    def wisski_entities(self):
        return list(entity_uri(search_value="",
                               query_string=self._query.get(self._bundle_dict.get(self._bundle_name)['query']),
                               return_format='csv', value_input=False).iloc[:, 0])

    # Check missing values
    def checker(self) -> list | list[dict]:
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
                if self._sync_field_name in self._double_fields:
                    entity_value[self._field.get(self._bundle_dict.get(self._bundle_name)['alt_field'])] = [
                        jm.search(f"[?name=='{entity}'].identifier")[0]
                    ]
                self._staged_values.append(entity_value)
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

    def staged(self):
        if not self.check_missing() == []:
            for entity in self.check_missing():
                entity_value = {
                    self._field.get(self._bundle_dict.get(self._bundle_name)['field']): [entity]
                }
                if self._sync_field_name in self._double_fields:
                    entity_value[self._field.get(self._bundle_dict.get(self._bundle_name)['alt_field'])] = [
                        jm.search(f"[?name=='{entity}'].identifier")[0]
                    ]
                self._staged_values.append(entity_value)
        return self._staged_values

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