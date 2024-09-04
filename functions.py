# -*- coding: utf-8 -*-
"""
Created on Tue May 28 15:17:50 2024

@author: AfricaMultiple
"""

# Library Imports
import pandas as pd
import numpy as np
from SPARQLWrapper import SPARQLWrapper, JSON, CSV
import io
import json



# Function for the entity retrieval
# This function checks for the existence of entity and return the WissKI for the same,
# Incase entity does not exist, the function return the np.nan


def load_config():
    with open('functions_config.json') as config_file:
        return json.load(config_file)


def entity_uri(search_value: str | dict[str, str],
               query_string: str,
               return_format='json',
               value_input=True,
               conditional=False) -> str | object | None:
    # Load graphdb sparql configuration
    config = load_config()
    format_dict = {'json': JSON, 'csv': CSV}
    sparql = SPARQLWrapper(config['sparql_endpoint'])
    sparql.setHTTPAuth('BASIC')
    sparql.setCredentials(config['sparql_username'], config['sparql_password'])
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



# Try Function (NameError)


def try_func(value, func):
    try:
        return func(value)
    except NameError:
        return value
