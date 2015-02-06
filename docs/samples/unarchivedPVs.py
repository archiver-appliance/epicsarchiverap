#!/usr/bin/env python
'''Given a list of comma separated PVs, determine all the PV's that are not being archived.'''

import os
import sys
import argparse
import time
import urllib
import urllib2
import json
import datetime
import time

def getUnarchivedPVs(bplURL, pvNames):
    '''Of the PVs in the list, determine those that are unarchived. '''
    url = bplURL + '/unarchivedPVs'
    data = "pv=" + ",".join(pvNames)
    req = urllib2.Request(url, data)
    response = urllib2.urlopen(req)
    the_page = response.read()
    retval = json.loads(the_page)
    return retval



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("file", help="A comma separated list, the first column is the PV name.")
    args = parser.parse_args()
    lines = []
    with open(args.file, 'r') as f:
        lines = f.readlines()
    pvNames = []
    pvName2Conf = {}
    for line in lines:
        line = line.strip()
        parts = line.split(",")
        pvName = parts[0]
        pvName2Conf[pvName] = line
        pvNames.append(urllib.quote_plus(pvName))
    unarchivedPVs = getUnarchivedPVs(args.url, pvNames)
    for unarchivedPV in sorted(unarchivedPVs):
        print pvName2Conf[unarchivedPV]
        
        
    