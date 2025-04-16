# -*- coding: utf-8 -*-
"""
Created on Tue May 28 15:17:50 2024

@author: AfricaMultiple
"""

# Library Imports
import io
import json
from typing import Callable, NamedTuple, Union, MutableMapping

from pathlib import Path

import pandas as pd
from pymongo import MongoClient
from SPARQLWrapper import CSV, JSON, SPARQLWrapper

# Function for fetching all documents belong to a DB and Collection


def load_config(config_file='dicts/functions_config.json'):
    config_path = Path(config_file)
    try:
        with config_path.open() as f:
            return json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found: {config_path}")
    except json.JSONDecodeError:
        raise ValueError(f"Invalid JSON in config file: {config_path}")


def mongodata_fetch(db_name, collection_name, as_list: bool = True, filter_str: dict | None = None):
    config = load_config()
    client = MongoClient(config['mongo_uri'])
    db = client[db_name]
    collection = db[collection_name]
    if as_list:
        if filter_str:
            return list(collection.find(filter_str))
        else:
            return list(collection.find())
    else:
        return collection


# Function for the entity retrieval
# This function checks for the existence of entity and return the WissKI for the same,

# In case entity does not exist, the function return the np.nan
def entity_uri(
    search_value: Union[str, NamedTuple],
    query_string: str,
    return_format="json",
    value_input=True,
    conditional=False,
    cache: MutableMapping[str, str] = {},
) -> str | object | None:
    cache_key = f"{search_value}_{hash(query_string)}"
    if cache_key in cache:
        return cache[cache_key]

    # Load graphdb sparql configuration
    config = load_config()
    format_dict = {'json': JSON, 'csv': CSV}
    sparql = SPARQLWrapper(config['sparql_endpoint'])
    sparql.setReturnFormat(format_dict[return_format])
    sparql.setHTTPAuth('BASIC')
    sparql.setCredentials(config['sparql_username'], config['sparql_password'])
    
    if value_input:
        if not conditional:
            # If search_value is a string, escape any special characters for SPARQL
            if isinstance(search_value, str):
                # Double quotes and backslashes need to be escaped in SPARQL
                escaped_value = search_value.replace('\\', '\\\\').replace('"', '\\"')
                
                # Use the escaped value in the query
                formatted_query = query_string.format(search_value=escaped_value)
            else:
                formatted_query = query_string.format(search_value=search_value)
                
            sparql.setQuery(formatted_query)
        elif conditional:
            assert isinstance(search_value, tuple)
            # Escape any string values in the tuple
            escaped_dict = {}
            for key, value in search_value._asdict().items():
                if isinstance(value, str):
                    escaped_dict[key] = value.replace('\\', '\\\\').replace('"', '\\"')
                else:
                    escaped_dict[key] = value
                    
            sparql.setQuery(query_string.format(**escaped_dict))
    elif not value_input:
        sparql.setQuery(query_string)

    query_response = sparql.queryAndConvert()
    
    if return_format == 'json':
        try:
            val = str(query_response["results"]["bindings"][0]["id"]["value"])
            cache[cache_key] = val
            return val
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
