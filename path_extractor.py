# XML & pandas Library
from urllib.parse import urlparse
from datetime import datetime
import wget
import json
import glob
import os
import time


def pathbuilder_save(xml_file_name=max(glob.glob("pathbuilder/*"), key=os.path.getctime)):

    if os.path.isfile(xml_file_name):
        tree = eT.parse(xml_file_name)
    elif urlparse(xml_file_name).netloc:
        wget.download(xml_file_name, out=f"pathbuilder/pb_{datetime.now().strftime('%Y%m%d%H%M%S')}.xml")
        time.sleep(20)
        latest = max(glob.glob("pathbuilder/*"), key=os.path.getctime)
        tree = eT.parse(latest)

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
