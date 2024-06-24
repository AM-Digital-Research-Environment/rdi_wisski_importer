import csv
# ~ import json
import pickle
import requests

class GeonamesWikidataResolver():
  
  def __init__(self, countries_and_states_file = None):
    self.wikidata_to_countries = {}
    self.wikidata_to_states = {}
    self.countries = {}
    self.regions = {}
    self.locations = set()
    self.wikidata_query = """SELECT ?id WHERE {      
  ?id wdt:P1566 "%s".      
  SERVICE wikibase:label {
      bd:serviceParam wikibase:language "en" .
  }
}"""
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
          self.wikidata_to_states[row['state']] = row['stateLabel']
    
  def collect_location_ids_from_file(self, filename):
    # ~ self.locations.add('18613')
    # ~ self.locations.add('5768')
    keys_cntr = self.countries.keys()
    keys_rgn = self.regions.keys()
    with open(filename, 'r') as _csv:
      easydb = csv.DictReader(_csv, delimiter=';', quotechar='"')
      for row in easydb:
        loc = row['placeoforigin_geographical#_system_object_id']
        if loc != '' and not (loc in keys_cntr or loc in keys_rgn):
          self.locations.add(loc)

  def resolve_geonames(self):
    for loc_id in self.locations:
      url = f"https://collections.uni-bayreuth.de/api/v1/objects/id/{loc_id}?token=31e2e944-af77-45bf-937e-43cb3839805c"
      easydbloc = requests.get(url)
      if easydbloc.status_code == 200:
        loc = easydbloc.json()
        if not 'location' in loc or not 'geoname' in loc['location'] or not 'conceptURI' in loc['location']['geoname']:
          print(f"couldn't fetch required fields for {loc_id}. to check yourself: {url}")
          continue
        geoid = (loc['location']['geoname']['conceptURI']).split('/')[-1]
        wikidata = requests.get('https://query.wikidata.org/sparql', params = {'format': 'json', 'query': self.wikidata_query % geoid})
        if wikidata.status_code == 200:
          result = wikidata.json()
          if result['results']['bindings'] != []:
            wikidata_id = result['results']['bindings'][0]['id']['value']
            print(loc['_system_object_id'],wikidata_id)
            if wikidata_id in self.wikidata_to_countries.keys():
              self.countries[loc['_system_object_id']] = self.wikidata_to_countries[wikidata_id]
            if wikidata_id in self.wikidata_to_states.keys():
              self.regions[loc['_system_object_id']] = self.wikidata_to_states[wikidata_id]
  
  def get_country_mappings(self):
    return self.countries
  
  def get_region_mappings(self):
    return self.regions

  def save(self, outfile):
    with open(outfile, 'wb') as _out:
      pickle.dump({
        'countries': self.countries,
        'regions'  : self.regions,
        'wd2cntr'  : self.wikidata_to_countries,
        'wd2rgn'   : self.wikidata_to_states
      }, _out, protocol=pickle.HIGHEST_PROTOCOL)

  def load(self, infile):
    with open(infile, 'rb') as _in:
      b = pickle.load(_in)
      self.countries             = b['countries']
      self.regions               = b['regions']
      self.wikidata_to_countries = b['wd2cntr']
      self.wikidata_to_states    = b['wd2rgn']
    print('restored location mappings from',infile)
    print('countries :',self.countries)
    print('regions :',self.regions)
    return self

  # ~ def toJSON(self):
    # ~ return json.dumps(
      # ~ self,
      # ~ default=lambda o: o.__dict__, 
      # ~ sort_keys=True,
      # ~ indent=4)
