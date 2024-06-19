# -*- coding: utf-8 -*-
"""
Created on Tue May 28 15:17:50 2024

@author: AfricaMultiple
"""

# Library Imports
import pandas as pd
import numpy as np
from typing import Callable
from pymongo import MongoClient
from SPARQLWrapper import SPARQLWrapper, JSON, CSV
import io
import json


# Function for fetching all documents belong to a DB and Collection


def mongodata_fetch(db_name, collection_name):
    client = MongoClient("***REMOVED***")
    db = client[db_name]
    collection = db[collection_name]
    return list(collection.find())


# Function for the entity retrieval
# This function checks for the existence of entity and return the WissKI for the same,
# Incase entity does not exist, the function return the np.nan
# Repo used: "***REMOVED******REMOVED***" (WissKI_89)


def entity_uri(search_value: str | dict[str, str],
               query_string: str,
               return_format='json',
               value_input=True,
               conditional=False) -> str | object | None:
    format_dict = {'json': JSON, 'csv': CSV}
    sparql = SPARQLWrapper("***REMOVED******REMOVED***")
    sparql.setReturnFormat(format_dict[return_format])
    if value_input:
        if not conditional:
            sparql.setQuery(query_string.format(search_value=search_value))
        elif conditional:
            sparql.setQuery(query_string.format(**search_value))
    elif not value_input:
        sparql.setQuery(query_string)
    query_response = sparql.queryAndConvert()
    if return_format == 'json':
        try:
            return query_response["results"]["bindings"][0]['id']['value']
        except IndexError:
            return None
    elif return_format == 'csv':
        try:
            return pd.read_csv(io.StringIO(query_response.decode('utf-8')))
        except IndexError:
            return None

# Function for the json data retrieval


def json_file(file_path: str):
    with open(file_path) as file_obj:
        try:
            return json.load(file_obj)
        finally:
            file_obj.close()

# Function for the entity generation of single
# TODO: Exception function as argument incase entity dies not existing the system.


def entity_list_generate(value_list, query_name, exception_function: Callable, with_exception=False):
    entity_list = []
    for entity_value in value_list:
        uri_value = entity_uri(entity_value, query_name)
        if uri_value is None:
            if with_exception:
                entity_list.append(exception_function(entity_value=entity_value))
            elif not with_exception:
                entity_list.append(entity_value)
        else:
            entity_list.append(uri_value)
    return entity_list

# Try Function (NameError)


def try_func(value, func):
    try:
        return func(value)
    except NameError:
        return value