# XML & pandas Library
import xml.etree.ElementTree as eT
import json

tree = eT.parse(r'pathbuilder\amo_ecrm__v01_dev_pb_20240604T153133.xml')
root = tree.getroot()

bundles = {}
fields = {}

for path in root.findall('path'):
    if path.find('is_group').text == '1':
        bundles[path.find('id').text] = path.find('bundle').text
    elif path.find('is_group').text == '0':
        fields[path.find('id').text] = path.find('field').text

# Generating json files
with open('dicts\\bundles.json', 'w') as bfile:
    json.dump(bundles, bfile)

with open('dicts\\fields.json', 'w') as ffile:
    json.dump(fields, ffile)
