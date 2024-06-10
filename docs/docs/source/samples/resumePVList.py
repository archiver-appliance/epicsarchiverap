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

def resumePVs(bplURL, pvNames):
    '''Resumes the pvs specified by list pvNames'''
    url = bplURL + '/resumeArchivingPV'
    req = urllib2.Request(url)
    req.add_header('Content-Type', 'application/json')
    response = urllib2.urlopen(req, json.dumps(pvNames))
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
    pvNames = []
    for line in lines:
        pvName = line.strip()
        pvNames.append(pvName)

    resumeResponse = resumePVs(args.url, pvNames)
    for pvResponse in resumeResponse:
        print "{0} => {1}".format(pvResponse['pvName'], pvResponse['status'] if 'status' in pvResponse else pvResponse['validation'])
