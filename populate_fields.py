#!/usr/bin/env python3

import logging
from pathlib import Path
import os.path
import pandas as pd

import json
import csv
from entity_builder_easydb import *
from path_extractor import pathbuilder_save

BASE = Path(__file__).stem
logger = logging.getLogger(BASE)

def main(args):
  logger.info(args)
  filename = max([f for f in Path('./pathbuilder').glob('amo_ecrm__v01_dev_pb*.xml')], 
               key=os.path.getctime)
  print(f"updating field ids from {filename}")
  pathbuilder_save(Path(filename).resolve())
  
  authoritynames = set()
  authorities = []
  authorityrolenames = set()
  authoritiy_roles = []
  
  MARC_roles = set()
  with open(args.marc_role_file, 'r') as _marc_f:
    _marc_f.readline()
    for line in _marc_f:
      MARC_roles.add((line.split('\t')[1]).strip())
  
  with open(args.authority_file, 'r') as _auth_f:
    _auth_f.readline()
    for line in _auth_f:
      line = line.split('\t')
      name = (line[0]).strip()
      if name in authoritynames:
        continue
      authoritynames.add(name)
      authorities.append({'f_authority_name': name, 'f_authority_url': (line[1]).strip()})
 
  es = EntitySync('identifiers', list(authoritynames))
  es.update_multiple_values(authorities, 'f_authority_name')
  
  marc_uri = es.entity_uri(search_value='Machine-Readable Cataloging', query_id='identifier')
  with open(args.authority_role_file, 'r') as _authr_f:
    _authr_f.readline()
    for line in _authr_f:
      line = line.split('\t')[0]
      role = (line[0]).strip()
      if name in authorityrolenames:
        continue
      authorityrolenames.add(role)
      if role in MARC_roles:
        authoritiy_roles.append({'f_authority_role_name': role, 'f_authority_role_source': marc_uri})
      else
        authoritiy_roles.append({'f_authority_role_name': role})
  
  EntitySync('authorityroles', list(authorityrolenames)).update_multiple_values(authoritiy_roles, 'f_authority_role_name')
  
  

def get_log_filename(out_dir, base, ext='.log'):
  def get_basename(log_id, base, ext='.log'):
    return '{}_{:d}{}'.format(base,log_id,ext)
  
  log_count = 0
  f = Path(out_dir, get_basename(log_count, base))
  while f.exists():
    log_count += 1
    f = Path(out_dir, get_basename(log_count, base))
  return f.resolve()

def configure_logging(args, base):
  if args.log_to_file:
    p = Path(args.log_dir)
    p.mkdir(parents=True, exist_ok=True)
  for handler in logging.root.handlers:
    logging.root.removeHandler(handler)
  logging.root.handlers = []
  logging.root.setLevel(args.log_level)
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  
  if args.log_to_file:
    logfile = logging.FileHandler(get_log_filename(args.log_dir, base=base))
    logfile.setFormatter(formatter)
    logging.root.addHandler(logfile)

  if not args.log_to_console_off:
    logconsole = logging.StreamHandler()
    logconsole.setFormatter(formatter)
    logging.root.addHandler(logconsole)

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser(description="Run this thing.")

  parser.add_argument(
    '-d', '--debug',
    help="Print lots of debugging statements",
    action="store_const", dest="log_level", const=logging.DEBUG,
    default=logging.WARNING)
  
  parser.add_argument(
    '-v', '--verbose',
    help="Be verbose",
    action="store_const", dest="log_level", const=logging.INFO)
                      
  parser.add_argument(
    '--log_to_file', 
    action='store_true',
    help='Redirect log to file')
                      
  parser.add_argument(
    '--log_to_console_off', 
    action='store_true',
    help='Turn off console output')
                      
  parser.add_argument(
    '--log_dir', 
    nargs='?', 
    default='logs',
    help='choose logging directory')
                      
  parser.add_argument(
    '--authority_file', 
    help='Authorities')
                      
  parser.add_argument(
    '--authority_role_file', 
    help='Authority-defined Roles')
                      
  parser.add_argument(
    '--marc_role_file', 
    help='MARC-defined Roles')
    
  # ~ parser.add_argument(
    # ~ 'csv', 
    # ~ help='csv',nargs=argparse.REMAINDER)
                      
  args = parser.parse_args()
  configure_logging(args, BASE)
  
  main(args)

