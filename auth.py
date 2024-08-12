# Local Functions
from functions import *

# WissKi Api
from wisski.api import Api, Pathbuilder, Entity

# General entity management variable class


class GeneralEntity:

    def __init__(self):

        # Dictionary Objects
        self._bundle = json_file("dicts/bundles.json")
        self._field = json_file("dicts/fields.json")
        self._query = json_file("dicts/sparql_queries.json")
        self._language = json_file("dicts/lang.json")

        # WissKI Auth
        self._api_url = "http://www.wisski.uni-bayreuth.de/wisski/api/v0"
        self._auth = ("***REMOVED***", "***REMOVED***")
        self._api = Api(self._api_url, self._auth, {"Cache-Control": "no-cache"})
        self._api.pathbuilders = ["amo_ecrm__v01_dev_pb"]