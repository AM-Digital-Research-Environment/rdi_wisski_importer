from functions import json_file


class GeneralEntity:
    """
    General entity management class
    """

    def __init__(self):

        # Dictionary Objects
        self._bundle = json_file("dicts/bundles.json")
        self._field = json_file("dicts/fields.json")
        self._query = json_file("dicts/sparql_queries.json")
        self._language = json_file("dicts/lang.json")
