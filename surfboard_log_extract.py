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
def ExtractLogs(session, url):
    fields = []
    response = session.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    for table in soup.find_all('table'):
        # Iterate through the logs table, row by row
        if 'Priority' in table.get_text() and len(table.find_all('tr')) > 1:
            for tr in table.find_all('tr'):
                ftmp = []
                if 'Priority' not in tr.get_text():
                    ftmp.append(tr.find_all('td')[0].get_text())
                    ftmp.append(tr.find_all('td')[1].get_text())
                    ftmp.append(tr.find_all('td')[2].get_text())
                    ftmp.append(tr.find_all('td')[3].get_text())
                if len(ftmp) > 0:
                    fields.append(ftmp)
            # fix timestamps
            # drop duplicate entries
            # convert to dict
    print fields
    return fields


def main():
    this_run_ts = datetime.utcnow().isoformat()
    # This is a dictionary to compile the stuff we will end up sending up to elasticsearch
    fields = []
    # Set up a session for our pull from the modem
    s = requests.session()

    fields.append(ExtractLogs(s, 'http://192.168.100.1/cmLogsData.htm'))
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

