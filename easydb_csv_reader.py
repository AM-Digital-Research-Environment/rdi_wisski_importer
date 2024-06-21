#!/usr/bin/env python3

import logging
from pathlib import Path
import os.path
import pandas as pd

import csv
from entity_builder_easydb import *
from path_extractor import pathbuilder_save

BASE = Path(__file__).stem
logger = logging.getLogger(BASE)

def main(args):
  logger.info(args)
  filename = max([f for f in Path('./pathbuilder').glob('amo_ecrm__v01_dev_pb*.xml')], 
               key=os.path.getctime)
               
  pathbuilder_save(Path(filename).resolve())
  # ~ return
  with open(args.csv[0], 'r') as _csv:
    easydb = csv.DictReader(_csv, delimiter=';', quotechar='"')
    n_fields = len(easydb.fieldnames)
    
    # 1. create persons
    
    
    
    for row in easydb:
      assert(len(row.keys()) == n_fields)
      ent = EasydbEntity(row)
      print(ent.staging())


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

