#!/usr/bin/env python
import sys
import requests
import json
from datetime import datetime
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch


epoch = datetime.utcfromtimestamp(0)
this_run_ts = datetime.now()
this_run_ts_millis = (this_run_ts - epoch).total_seconds() * 1000.0

# These are the pages from the modem and fields we want to record
#  Signals is a complicated enough page that I may break that out to a different process.
datapulls = {}
datapulls['http://192.168.100.1/cmHelpData.htm'] = ['Model Name','Vendor Name', 'Firmware Name', 'Boot Version', 'Hardware Version', 'Serial Number', 'Firmware Build Time']

# This is a dictionary to compile the stuff we will end up sending up to elasticsearch
fields = {}
fields['timestamp'] = this_run_ts_millis

# We are going to store our results in Elasticsearch.
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
# Set up a session for our pull from the modem
s = requests.session()

# Loop through the pages we want to snag data from
for url in datapulls.keys():
    qfields = datapulls[url]
    response = s.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    # Loop through td tags
    for td in soup.find("td"):
        for qfield in qfields:
            if qfield in td:
                s = str(td)
                s = s.split(':',1)
                s = map(str.strip, s)
                fields[s[0]] = s[1]

print fields
