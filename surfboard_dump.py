#!/usr/bin/env python
import sys
import requests
import json
import pprint
from datetime import datetime
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

epoch = datetime.utcfromtimestamp(0)
this_run_ts = datetime.now()
this_run_ts_millis = (this_run_ts - epoch).total_seconds() * 1000.0

# Looks for a list of strings in the body of a page
#  Returns a dict of the found strings and their value after the ':'
def ExtractStringList(session, url, sl):
    fields = {}
    response = session.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    for line in soup.body.get_text().split('\n'):
        for needle in sl:
            if needle in line:
                line = line.split(':',1)
                line = map(unicode.strip, line)
                fields[line[0]] = line[1]
    return fields

# Takes a session and url
#  Returns a dict comprised key:values from tables on the page
def ExtractFromTables(session, url):
    fields = {}
    response = session.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    for table in soup.findAll('table'):
        for tr in table.findAll('tr'):
            td=tr.findAll('td')
            # This looks like a key:value pair
            if len(td) == 2:
                key = td[0].get_text().strip()
                value = td[1].get_text().strip()
                fields[key] = value
    return fields


def main():
    # This is a dictionary to compile the stuff we will end up sending up to elasticsearch
    fields = {}
    fields['timestamp'] = this_run_ts_millis
    # Set up a session for our pull from the modem
    s = requests.session()


    # Grab from the Help page
    fields.update(ExtractStringList(s,'http://192.168.100.1/cmHelpData.htm', ['Model Name','Vendor Name', 'Firmware Name', 'Boot Version', 'Hardware Version', 'Serial Nu    mber', 'Firmware Build Time']))
    fields.update(ExtractFromTables(s, 'http://192.168.100.1/indexData.htm'))
    s.close()
    pprint.pprint(fields)
    # We are going to store our results in Elasticsearch.
    #es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


if __name__ == '__main__':
    main()

