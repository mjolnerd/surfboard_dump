#!/usr/bin/env python

'''
Map this using:
PUT surfboard6141_logs
{
  "mappings": {
    "logs":{
      "properties": {
        "timestamp":{
          "type": "date",
          "format": "MMM d yyyy HH:mm:ss"
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
from dateutil import tz
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

# Takes a session and url
#  Returns a dict with sensible structure for the Logs page
#  We don't have access to a flatfile text log, because Cox. So there will
#  be some shenanigans to avoid duplicates.
def ExtractLogs(session, url, last_ts):
    fields = []
    response = session.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    for table in soup.find_all('table'):
        # Iterate through the logs table, row by row
        if 'Priority' in table.get_text() and len(table.find_all('tr')) > 1:
            for tr in table.find_all('tr'):
                ftmp = {}
                if 'Priority' not in tr.get_text():
                    ftmp['timestamp'] = tr.find_all('td')[0].get_text()
                    ftmp['priority'] = tr.find_all('td')[1].get_text()
                    ftmp['code'] = tr.find_all('td')[2].get_text()
                    ftmp['message'] = tr.find_all('td')[3].get_text()
                if len(ftmp) > 0:
                    fields.append(ftmp)

            last_ts = 'NULL'
            # Moving from oldest to newest
            for entry in reversed(fields):
                ts = datetime.strptime(entry['timestamp'],'%b %d %Y %H:%M:%S')
                if str(ts.year) == '1970':
                    epoch = datetime.utcfromtimestamp(0)
                    ts_offset = ts - epoch
                    # Oops, we don't have a last_ts yet.
                    # Look for a more recent timestamp and work backwards from there.
                    if str(last_ts) == 'NULL':
                        for i in reversed(fields):
                            tc = datetime.strptime(i['timestamp'],'%b %d %Y %H:%M:%S')
                            if str(tc.year) == '1970':
                                pass
                            else:
                                last_ts = tc
                                break
                        # Still nothing?  Ok, manually set it to now.
                        if str(last_ts) == 'NULL':
                            last_ts = datetime.now()
                        # Offset backwards from future ts
                        ts = last_ts - ts_offset
                        # Reset this so these don't stack (yes it is inefficient, CPU is cheap, for now...)
                        last_ts = 'NULL'
                    else:
                        # Offset forwards from past ts
                        ts = last_ts + ts_offset
                else:
                    last_ts = ts
                # Now that is sorted, lets make this timezone aware
                tz_loc = tz.tzlocal()
                ts = ts.replace(tzinfo = tz_loc)
                tsutc = ts.astimezone(tz.tzutc())
                entry['timestamp_utc'] = tsutc.isoformat()
                #print 'ts: {} / ts_utc: {}'.format(ts, entry['timestamp_utc'])

            # drop duplicate entries

    return fields


def main():
    fields = []
    # Get the timestamp for the last entry we placed into elasticsearch
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


    # Set up a session for our pull from the modem
    s = requests.session()

    fields = ExtractLogs(s, 'http://192.168.100.1/cmLogsData.htm','catfood')
    s.close()

    #pprint.pprint(fields)
    json_string = json.dumps(fields[0])
    print json_string
    #Lets hold off on making our es indexes icky until we test this...
    #es.index(index = 'surfboard6141_logs', doc_type = 'logs', body = json_string)
# END def MAIN

if __name__ == '__main__':
    main()

