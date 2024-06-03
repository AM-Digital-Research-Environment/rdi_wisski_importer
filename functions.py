# -*- coding: utf-8 -*-
"""
Created on Tue May 28 15:17:50 2024

@author: AfricaMultiple
"""

# Library Imports
import pandas as pd
import numpy as np
from pymongo import MongoClient
from SPARQLWrapper import SPARQLWrapper, JSON, CSV
import io


# Function for fetching all documents belong to a DB and Collection


def mongodata_fetch(db_name, collection_name):
    client = MongoClient("mongodb://bot:WissKI%401231@132.180.10.89:27017/?authMechanism=DEFAULT")
    db = client[db_name]
    collection = db[collection_name]
    return list(collection.find())


# Function for the entity retrieval
# This function checks for the existence of entity and return the WissKI for the same,
# Incase entity does not exist, the function return the np.nan
# Repo used: "http://132.180.10.160:7200/repositories/amo_data" (WissKI_89)


def entity_uri(query_string, return_format='json'):
    format_dict = {'json': JSON, 'csv': CSV}
    sparql = SPARQLWrapper("http://132.180.10.160:7200/repositories/amo_data")
    sparql.setReturnFormat(format_dict[return_format])
    sparql.setQuery(query_string)
    query_response = sparql.queryAndConvert()
    if return_format == 'json':
        return query_response["results"]["bindings"]
    elif return_format == 'csv':
        return pd.read_csv(io.StringIO(query_response.decode('utf-8')))