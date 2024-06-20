# XML & pandas Library
import xml.etree.ElementTree as eT
import json


def pathbuilder_save(xml_file_name):
    tree = eT.parse(xml_file_name)
    root = tree.getroot()

    bundles = {}
    fields = {}

    for path in root.findall('path'):
        if path.find('is_group').text == '1':
            bundles[path.find('id').text] = path.find('bundle').text
        elif path.find('is_group').text == '0':
            fields[path.find('id').text] = path.find('field').text

    # Generating json files
    with open('dicts/bundles.json', 'w') as bfile:
        json.dump(bundles, bfile)

    with open('dicts/fields.json', 'w') as ffile:
        json.dump(fields, ffile)

    return "Success!"
