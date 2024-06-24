import csv
# ~ import json
import pickle
import requests

class GeonamesWikidataResolver():
  
  def __init__(self, countries_and_states_file = None):
    self.wikidata_to_countries = {}
    self.wikidata_to_states = {}
    self.mappings = {}
    self.locations = set()
    self.wikidata_query = """SELECT ?id ?idLabel WHERE {      
  ?id wdt:P1566 "%s".      
  SERVICE wikibase:label {
      bd:serviceParam wikibase:language "en" .
  }
}"""
    self.collections_query = "https://collections.uni-bayreuth.de/api/v1/objects/id/%s?token=31e2e944-af77-45bf-937e-43cb3839805c"
    if not countries_and_states_file is None:
      self.populate_countries_and_states_dict(countries_and_states_file)
   
  def populate_countries_and_states_dict(self, countries_and_states_file):
    with open(countries_and_states_file, 'r') as _csv:
      easydb = csv.DictReader(_csv, delimiter='\t')
      n_fields = len(easydb.fieldnames)
      for row in easydb:
        if row['country'] not in self.wikidata_to_countries.keys():
          self.wikidata_to_countries[row['country']] = row['countryLabel']
        if row['state'] not in self.wikidata_to_states.keys():
          self.wikidata_to_states[row['state']] = {'state': row['stateLabel'], 'country':row['countryLabel']}
    
  def collect_location_ids_from_file(self, filename):
    # ~ self.locations.add('18613')
    # ~ self.locations.add('5768')
    known_keys = self.mappings.keys()
    locations = set()
    with open(filename, 'r') as _csv:
      easydb = csv.DictReader(_csv, delimiter=';', quotechar='"')
      for row in easydb:
        loc = row['placeoforigin_geographical#_system_object_id']
        locations.add(loc)
    self.locations = locations
  
  def query_wikidata(self, geoid):
    wikidata = requests.get('https://query.wikidata.org/sparql', params = {'format': 'json', 'query': self.wikidata_query % geoid})
    if wikidata.status_code == 200:
      result = wikidata.json()
      if result['results']['bindings'] != []:
        wikidata_id = result['results']['bindings'][0]['id']['value']
        label = result['results']['bindings'][0]['idLabel']['value']
        return True, wikidata_id, label
    return False, None, None

  def query_easydb(self, easydbid):
    url = self.collections_query % easydbid
    easydbloc = requests.get(url)
    if easydbloc.status_code == 200:
      loc = easydbloc.json()
      if not 'location' in loc or not 'geoname' in loc['location'] or not 'conceptURI' in loc['location']['geoname']:
        print(f"couldn't fetch required fields for {easydbid}. to check yourself: {url}")
        return False, None, None
      geoid = (loc['location']['geoname']['conceptURI']).split('/')[-1]
      success, wikidata_id, label = self.query_wikidata(geoid)
      if success:
        return True, wikidata_id, label
    return False, None, None
  
  def resolve_geonames(self):
    for loc_id in self.locations:
      url = self.collections_query % loc_id
      easydbloc = requests.get(url)
      if easydbloc.status_code == 200:
        loc = easydbloc.json()
        if not '_path' in loc:
          print(f"couldn't fetch required fields for {loc_id}. to check yourself: {url}")
          continue
        
        path = []
        for level,location_obj in enumerate(loc['_path']):
          path.append(location_obj['_system_object_id'])
        
        # ~ print(loc_id, path)
        if len(path) >= 0 and not path[0] in self.mappings:
          success, wikidata_id, label = self.query_easydb(path[0])
          if success and label is not None and wikidata_id.find(label) == -1:
            self.mappings[path[0]] = {'country':label, 'wd':wikidata_id}
        
        if len(path) > 1 and not path[1] in self.mappings:
          success, wikidata_id, label = self.query_easydb(path[1])
          if success and label is not None and wikidata_id.find(label) == -1:
            self.mappings[path[1]] = {'country':self.mappings[path[0]]['country'], 'region':label, 'wd':wikidata_id}
            if path[1] == 18810:
              self.mappings[path[1]] = {'country':self.mappings[path[0]]['country'], 'region':'Zinder Region', 'place':label, 'wd':wikidata_id}
        
        if len(path) > 2 and not path[2] in self.mappings:
          success, wikidata_id, label = self.query_easydb(path[2])
          # ~ print(loc_id, path, label, wikidata_id)
          if success and label is not None and wikidata_id.find(label) == -1:
            self.mappings[path[2]] = {'country':self.mappings[path[0]]['country'], 'subregion':label, 'wd':wikidata_id}
            if path[1] in self.mappings and 'region' in self.mappings[path[1]]:
              self.mappings[path[2]]['region'] = self.mappings[path[1]]['region']
            # ~ print("---->",self.mappings[path[2]])
        
        if len(path) > 3 and not path[3] in self.mappings:
          success, wikidata_id, label = self.query_easydb(path[3])
          # ~ print(loc_id, path, label, wikidata_id)
          if success and label is not None and wikidata_id.find(label) == -1:
            self.mappings[path[3]] = {'country':self.mappings[path[0]]['country'], 'place':label, 'wd':wikidata_id}
            if path[1] in self.mappings and 'region' in self.mappings[path[1]]:
              self.mappings[path[3]]['region'] = self.mappings[path[1]]['region']
            if path[2] in self.mappings and 'subregion' in self.mappings[path[2]]:
              self.mappings[path[3]]['subregion'] = self.mappings[path[2]]['subregion']
            # ~ print("---->",self.mappings[path[3]])
          
  def get_mappings(self):
    return self.mappings

  def save(self, outfile):
    with open(outfile, 'wb') as _out:
      pickle.dump({
        'mappings' : self.mappings,
        'wd2cntr'  : self.wikidata_to_countries,
        'wd2rgn'   : self.wikidata_to_states
      }, _out, protocol=pickle.HIGHEST_PROTOCOL)

  def load(self, infile):
    with open(infile, 'rb') as _in:
      b = pickle.load(_in)
      self.mappings              = b['mappings']
      self.wikidata_to_countries = b['wd2cntr']
      self.wikidata_to_states    = b['wd2rgn']
    print('restored location mappings from',infile)
    # ~ import json
    # ~ print('mappings :',json.dumps(self.mappings,indent=2))
    return self

