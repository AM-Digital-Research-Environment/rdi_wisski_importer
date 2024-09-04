# -*- coding: utf-8 -*-
"""
Created on Tue May 28 15:17:50 2024

@author: AfricaMultiple
"""

# Library Imports
import functools
import io
import json
from typing import Callable, NamedTuple, Union

import pandas as pd
from pymongo import MongoClient
from SPARQLWrapper import CSV, JSON, SPARQLWrapper

# Function for fetching all documents belong to a DB and Collection


# Fill in the MongoDBClient bot URI below
def mongodata_fetch(db_name, collection_name, as_list: bool = True):
    client = MongoClient("")
    db = client[db_name]
    collection = db[collection_name]
    if as_list:
        return list(collection.find())
    else:
        return collection


# Function for the entity retrieval
# This function checks for the existence of entity and return the WissKI for the same,
# Incase entity does not exist, the function return the np.nan

def load_config():
    with open('functions_config.json') as config_file:
        return json.load(config_file)

@functools.cache
def entity_uri(search_value: Union[str, NamedTuple],
               query_string: str,
               return_format='json',
               value_input=True,
               conditional=False) -> str | object | None:
    # IDEA: fetch all entity URIs and cache them
    # Load configuration
    config = load_config()
    format_dict = {'json': JSON, 'csv': CSV}
    sparql = SPARQLWrapper(config['sparql_endpoint'])
    sparql.setReturnFormat(format_dict[return_format])
    sparql.setHTTPAuth('BASIC')
    sparql.setCredentials(config['sparql_username'], config['sparql_password'])
    if value_input:
        if not conditional:
            sparql.setQuery(query_string.format(search_value=search_value))
        elif conditional:
            assert isinstance(search_value, tuple)
            sparql.setQuery(query_string.format(**search_value._asdict()))
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

def json_file(file_path: str):
    """
    Retrieve json files from disk.
    """
    with open(file_path) as file_obj:
        return json.load(file_obj)


def entity_list_generate(value_list, query_name, exception_function: Callable = None, with_exception=False):
    """
    Generate a list of wisski entities for given values.
    """
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
        if func(value) is None:
            return value
        else:
            return func(value)
    except NameError:
        return value
