#!/usr/bin/env python
'''This script gets a list of PVS that never connected and then aborts the archive request for these PVs'''

import os
import sys
import argparse
import time
import urllib
import urllib2
import json
import datetime
import time

def getNeverConnectedPVs(bplURL):
    '''Get a list of PVs that never connected at all'''
    url = bplURL + '/getNeverConnectedPVs'
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    the_page = response.read()
    neverConnectedPVs = json.loads(the_page)
    return neverConnectedPVs

def abortArchivingPV(bplURL, pv):
    '''Aborts the request for archiving a PV'''
    values = url_values = urllib.urlencode({'pv' : pv})
    url = bplURL + '/abortArchivingPV?' + values
    print "Aborting request for pv", pv, "using url", url
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    the_page = response.read()
    abortResponse = json.loads(the_page)
    return abortResponse
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")    
    args = parser.parse_args()
    if not args.url.endswith('bpl'):
        print "The URL needs to point to the mgmt bpl; for example, http://arch.slac.stanford.edu/mgmt/bpl. ", args.url
        sys.exit(1)
    neverConnectedPVs = getNeverConnectedPVs(args.url)
    print len(neverConnectedPVs), "PVs have never connected"
    for neverConnectedPV in neverConnectedPVs:
        abortResponse = abortArchivingPV(args.url, neverConnectedPV['pvName'])
        print abortResponse
        time.sleep(1)