#!/usr/bin/env python3

import logging
from pathlib import Path
import os.path
import pandas as pd

import copy
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
  
  # ~ return
  with open(args.csv[0], 'r') as _csv:
    easydb = csv.DictReader(_csv, delimiter=';', quotechar='"')
    n_fields = len(easydb.fieldnames)
    
    # 1. create persons
    persons = set()
    projects = set()
    for row in easydb:
      assert(len(row.keys()) == n_fields)
      p = row['creator#_standard#de-DE']
      if p != '':
        persons.add(p)
      p = row['context_funding#_standard#de-DE']
      if p != '':
        projects.add(p)
    
    es = EntitySync('persons', list(persons))
    es.update()
    api = es.get_api()
    known_entities = es.get_known_entities()
    es = EntitySync('projects', list(projects), api, known_entities)
    es.update()
    known_entities = es.get_known_entities()
    EntitySync('institutions', ['Collections@UBT'], api, known_entities)
    es.update()
    known_entities = es.get_known_entities()
    EntitySync('identifiers', ['Collections@UBT Identifier', 'Collections@UBT Inventory Number'], api, known_entities)
    es.update()
    known_entities = es.get_known_entities()
    
    # add project
    
    _csv.seek(0) # reset file pointer, re-iterater over the file
    next(easydb) # skip header
    errors = {}
    success = 0
    for i,row in enumerate(easydb):
      assert(len(row.keys()) == n_fields)
      ent = EasydbEntity(row, api, known_entities)
      known_entities = ent.get_known_entities()
      # ~ print(known_entities)
      # ~ print(ent.staging())
      # ~ print(json.dumps(ent.staging(), indent=2))
      try:
        ent.upload()
        success += 1
        print("successfully written",i+1)
      except:
        errors[str(i)] = row
        print("error in row",i+1)
      if i == 3:
        break
    
    with open('ERRORS.json', 'w') as out:
      out.write(json.dumps(errors))
    
    print(f"errors: {len(errors)}\tsuccess: {success}")
    


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
    'csv', 
    help='csv',nargs=argparse.REMAINDER)
                      
  args = parser.parse_args()
  configure_logging(args, BASE)
  
  main(args)

