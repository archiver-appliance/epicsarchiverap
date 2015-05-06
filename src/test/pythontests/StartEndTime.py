#!/usr/bin/env python



# This command takes in a pvname and a start and end time and varies the start and end time in one hour intervals and makes requests to the localhost getData 
# It then prints the number of events returned for each time interval

#// TODO Complete this unit test...

import sys
import urllib
import json
import xml.utils.iso8601
import datetime
import time


def countEvents(pvName, start, end):
    startTime = xml.utils.iso8601.parse(start)
    endTime   = xml.utils.iso8601.parse(end)
    stepsize = (endTime - startTime)/10
    print stepsize
    for step in range(10):
        params = {}
        params['pv'] = pvName
        params['from'] = datetime.fromtimestamp(startTime + stepsize*step).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        params['to'] = datetime.fromtimestamp(endTime).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        url = "http://localhost:17665/retrieval/data/getData.json?"+ urllib.urlencode(params);
        print "Asking for data from ",  url
        dataf = urllib.urlopen(url)
        data = json.load(dataf)
        if len(data) != 1:
            print "Error when fetching data from url ", url, " Got ", len(data), " elements."
        print len(data[0]['data'])

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "Usage: ", sys.argv[0], "<pvName> <startTime> <endTime>"
        print "For example: ", sys.argv[0], "mshankar:arch:sine 2012-05-31T17:23:51.000Z 2012-05-31T18:46:51.000Z"
        sys.exit(1)
    countEvents(sys.argv[1], sys.argv[2], sys.argv[3])
