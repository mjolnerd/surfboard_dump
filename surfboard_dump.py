#!/usr/bin/env python

'''
Map this using:
PUT surfboard6141
{
  "mappings": {
    "stats":{
      "properties": {
        "timestamp":{
          "type": "date",
          "format": "epoch_millis"
        }
      }
    }
  }
}
'''

import sys
import requests
import json
from datetime import datetime
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch


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
#  Returns a dict of key:values from tables on the page
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

# Takes a session and url
#  Returns a dict with sensible structure for the Signals page
#  This page has a unique layout, so this won't be as flexible as the other
#  functions.  Will end up returning a multi-level dictionary.
def ExtractSignals(session, url):
    fields = {}
    response = session.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    for table in soup.findAll('table'):
        head = 'Downstream'
        if head in table.get_text() and len(table.findAll('tr')) > 1:
            fields[head] = {}
            # Load up a list of lists with our data
            t_temp = []
            rcount = 0
            for tr in table.findAll('tr'):
                if tr.findAll('td'):
                    t_temp.append([])
                    for td in tr.findAll('td',recursive=False):
                        t_temp[rcount].append(td.get_text().strip())
                    # Doing this shenanigans because of a recursive table issue...
                    if len(t_temp[rcount]) < 2:
                        t_temp.pop(rcount)
                    rcount += 1
            # Transpose rows -> columns
            tt_temp = zip(*t_temp)
            labels = ['null',
                    'Frequency (Hz)',
                    'Signal to Noise Ratio (dB)',
                    'Downstream Modulation',
                    'Power Level (DBmV)']
            # Won't need these anymore...
            tt_temp.pop(0)
            # Step through each channel data and Store in fields
            for cdat in tt_temp:
                channel = int(cdat[0])
                fields[head][channel] = {}
                # Freq
                fc = 1
                val = int(cdat[fc].split(' ')[0])
                fields[head][channel][labels[fc]] = val
                # SNR
                fc = 2
                val = int(cdat[fc].split(' ')[0])
                fields[head][channel][labels[fc]] = val
                # Downstream Modulation
                fc = 3
                val = cdat[fc]
                fields[head][channel][labels[fc]] = val
                # Power Level
                fc = 4
                val = int(cdat[fc].split(' ')[0])
                fields[head][channel][labels[fc]] = val

        head = 'Upstream'
        if head in table.get_text() and len(table.findAll('tr')) > 1:
            fields[head] = {}
            # Load up a list of lists with our data
            t_temp = []
            rcount = 0
            for tr in table.findAll('tr'):
                if tr.findAll('td'):
                    t_temp.append([])
                    for td in tr.findAll('td',recursive=False):
                        t_temp[rcount].append(td.get_text().strip())
                    # Doing this shenanigans because of a recursive table issue...
                    if len(t_temp[rcount]) < 2:
                        t_temp.pop(rcount)
                    rcount += 1
            # Transpose rows -> columns
            tt_temp = zip(*t_temp)
            labels = ['null',
                    'Frequency (Hz)',
                    'Ranging Service ID',
                    'Symbol Rate (Msym/sec)',
                    'Power Level (DBmV)',
                    'Upstream Modulation',
                    'Ranging Status']
            # Won't need these anymore...
            tt_temp.pop(0)
            # Step through each channel data and Store in fields
            for cdat in tt_temp:
                channel = int(cdat[0])
                fields[head][channel] = {}
                # Freq
                fc = 1
                val = int(cdat[fc].split(' ')[0])
                fields[head][channel][labels[fc]] = val
                # Range Service ID
                fc = 2
                val = int(cdat[fc].split(' ')[0])
                fields[head][channel][labels[fc]] = val
                # Symbol Rate
                fc = 3
                val = float(cdat[fc].split(' ')[0])
                fields[head][channel][labels[fc]] = val
                # Power Level
                fc = 4
                val = int(cdat[fc].split(' ')[0])
                fields[head][channel][labels[fc]] = val
                # Upstream Modulation
                fc = 5
                val = cdat[fc].split('\n')
                fields[head][channel][labels[fc]] = val
                # Ranging Status
                fc = 6
                val = cdat[fc]
                fields[head][channel][labels[fc]] = val

        head = 'Signal Stats'
        if head in table.get_text() and len(table.findAll('tr')) > 1:
            fields[head] = {}
            # Load up a list of lists with our data
            t_temp = []
            rcount = 0
            for tr in table.findAll('tr'):
                if tr.findAll('td'):
                    t_temp.append([])
                    for td in tr.findAll('td',recursive=False):
                        t_temp[rcount].append(td.get_text().strip())
                    # Doing this shenanigans because of a recursive table issue...
                    if len(t_temp[rcount]) < 2:
                        t_temp.pop(rcount)
                    rcount += 1
            # Transpose rows -> columns
            tt_temp = zip(*t_temp)
            labels = ['null',
                    'Total Unerrored Codewords',
                    'Total Correctable Codewords',
                    'Total Uncorrectable Codewords']
            # Won't need these anymore...
            tt_temp.pop(0)
            # Step through each channel data and Store in fields
            for cdat in tt_temp:
                channel = int(cdat[0])
                fields[head][channel] = {}
                # Unerrored
                fc = 1
                val = int(cdat[fc])
                fields[head][channel][labels[fc]] = val
                # Correctable
                fc = 2
                val = int(cdat[fc])
                fields[head][channel][labels[fc]] = val
                # Uncorrectable
                fc = 3
                val = int(cdat[fc])
                fields[head][channel][labels[fc]] = val
    return fields


def main():
    epoch = datetime.utcfromtimestamp(0)
    this_run_ts = datetime.now()
    this_run_ts_millis = int((this_run_ts - epoch).total_seconds() * 1000.0)
    # This is a dictionary to compile the stuff we will end up sending up to elasticsearch
    fields = {}
    fields['timestamp'] = this_run_ts_millis
    # Set up a session for our pull from the modem
    s = requests.session()


    # Grab from the Help page
    helplabels=['Model Name',
            'Vendor Name',
            'Firmware Name',
            'Boot Version',
            'Hardware Version',
            'Serial Number',
            'Firmware Build Time']
    fields.update(ExtractStringList(s,'http://192.168.100.1/cmHelpData.htm', helplabels))
    table_pulls = ['http://192.168.100.1/indexData.htm',
            'http://192.168.100.1/cmAddressData.htm',
            'http://192.168.100.1/cmConfigData.htm']
    for url in table_pulls:
        fields.update(ExtractFromTables(s, url))
    fields.update(ExtractSignals(s, 'http://192.168.100.1/cmSignalData.htm'))
    s.close()


    # We are going to store our results in Elasticsearch.
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    json_string = json.dumps(fields)
    #pprint.pprint(fields)
    es.index(index = 'surfboard6141', doc_type = 'stats', body = json_string)
# END def MAIN

if __name__ == '__main__':
    main()

