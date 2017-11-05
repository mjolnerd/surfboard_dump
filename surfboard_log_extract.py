#!/usr/bin/env python

'''
Map this using:
PUT surfboard6141
{
  "mappings": {
    "stats":{
      "properties": {
        "timestamp":{
          "type": "date"
        }
      }
    }
  }
}
'''

import sys
import requests
import json
import pprint
from datetime import datetime
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

# Takes a session and url
#  Returns a dict with sensible structure for the Logs page
#  We don't have access to a flatfile text log, because Cox. So there will
#  be some shenanigans to avoid duplicates.
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
    this_run_ts = datetime.utcnow().isoformat()
    # This is a dictionary to compile the stuff we will end up sending up to elasticsearch
    fields = {}
    fields['timestamp'] = this_run_ts
    # Set up a session for our pull from the modem
    s = requests.session()

    fields.update(ExtractSignals(s, 'http://192.168.100.1/cmLogsData.htm'))
    s.close()


    # We are going to store our results in Elasticsearch.
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    #pprint.pprint(fields)
    json_string = json.dumps(fields)
    #Lets hold off on making our es indexes icky until we test this...
    #es.index(index = 'surfboard6141', doc_type = 'logs', body = json_string)
# END def MAIN

if __name__ == '__main__':
    main()

