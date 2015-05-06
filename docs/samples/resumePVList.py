#!/usr/bin/env python
'''Given a list of PVs, this resumes all the PVs in that list'''

import os
import sys
import argparse
import time
import urllib
import urllib2
import json
import datetime
import time

def resumePV(bplURL, pvName):
    '''Resumes the pv specified by pvName'''
    url = bplURL + '/resumeArchivingPV?pv=' + urllib.quote_plus(pvName)
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    the_page = response.read()
    resumePVResponse = json.loads(the_page)
    return resumePVResponse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("file", help="A file with a list of PVS, one PV per line")
    args = parser.parse_args()
    lines = []
    with open(args.file, 'r') as f:
        lines = f.readlines()
    for line in lines:
        pvName = line.strip()
        resumeResponse = resumePV(args.url, pvName)
        print "{0} has been resumed with status {1}".format(pvName, resumeResponse['status'] if 'status' in resumeResponse else "N/A")
        time.sleep(1.0)
    