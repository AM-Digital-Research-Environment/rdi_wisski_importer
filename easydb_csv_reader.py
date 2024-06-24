#!/usr/bin/env python3

import logging
from pathlib import Path
import os.path
import pandas as pd

import json
import csv
from entity_builder_easydb import *
from path_extractor import pathbuilder_save
from geonames_to_wikidata import GeonamesWikidataResolver

BASE = Path(__file__).stem
logger = logging.getLogger(BASE)


READ_IN_MAX = 3

def main(args):
  logger.info(args)
  filename = max([f for f in Path('./pathbuilder').glob('amo_ecrm__v01_dev_pb*.xml')], 
               key=os.path.getctime)
  print(f"updating field ids from {filename}")
  pathbuilder_save(Path(filename).resolve())
  
  geolocs_file = Path('./geoloc/countries_and_states.pickle')
  if not geolocs_file.exists():
    print("fetching geonames")
    location_resolver = GeonamesWikidataResolver(Path('./geoloc/countries_and_states.tsv'))
    location_resolver.collect_location_ids_from_file(args.csv[0])
    location_resolver.resolve_geonames()
    location_resolver.save(geolocs_file.resolve())
  else:
    print("restore geonames from",geolocs_file)
    location_resolver = GeonamesWikidataResolver().load(geolocs_file.resolve())
  
  with open(args.csv[0], 'r') as _csv:
    easydb = csv.DictReader(_csv, delimiter=';', quotechar='"')
    n_fields = len(easydb.fieldnames)
    for row in easydb:
      assert(len(row.keys()) == n_fields)
    print("syntactic check of csv passed")
    _csv.seek(0) # reset file pointer, re-iterater over the file
    next(easydb) # skip header
    
    
    print("synching persons, projects, institutions, and identifiers")
    persons = set()
    projects = set()
    for row in easydb:
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
    known_objects = EntitySync("easydb_objects", None, api)._wisski_entities
    
    return
    
    _csv.seek(0) # reset file pointer, re-iterater over the file
    next(easydb) # skip header
    print("start intake of data now")
    errors = {}
    success = 0
    for i,row in enumerate(easydb):
      ent = EasydbEntity(row, known_objects = known_objects, location_resolver=location_resolver, api=api, known_entities=known_entities)
      known_entities = ent.get_known_entities()
      if ent.get_already_in_db():
        errors[str(i)] = row
        print("error in row",i+1,"(already in db)")
        continue
      try:
        ent.upload()
        success += 1
        print("successfully written",i+1)
      except KeyboardInterrupt:
        print('Interrupted')
        try:
            save_errors(errors)
            sys.exit(130)
        except SystemExit:
            save_errors(errors)
            os._exit(130)
      except Exception as e:
        errors[str(i)] = row
        save_errors(errors)
        print("error in row",i+1,e.get_message())
        sleep(1)
      if success == READ_IN_MAX:
        break
    
    save_errors(errors)
    print(f"errors: {len(errors)}\tsuccess: {success}")
    
def save_errors(errors):
  with open('ERRORS.json', 'w') as out:
    out.write(json.dumps(errors))

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

