#!/usr/bin/env python
'''Given a list of PVs, this pauses all the PVs in that list'''

import os
import sys
import argparse
import time
import urllib
import urllib2
import json
import datetime
import time

def pausePVs(bplURL, pvNames):
    '''Pauses the pvs specified by the list pvNames'''
    url = bplURL + '/pauseArchivingPV'
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    response = urllib2.urlopen(req, json.dumps(pvNames))
    the_page = response.read()
    pausePVResponse = json.loads(the_page)
    return pausePVResponse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("file", help="A file with a list of PVS, one PV per line")
    args = parser.parse_args()
    lines = []
    with open(args.file, 'r') as f:
        lines = f.readlines()
    pvNames = []
    for line in lines:
        pvName = line.strip()
        pvNames.append(pvName)
    
    pauseResponse = pausePVs(args.url, pvNames)
    for pvResponse in pauseResponse:
        print "{0} => {1}".format(pvResponse['pvName'], pvResponse['status'] if 'status' in pvResponse else pvResponse['validation'])
