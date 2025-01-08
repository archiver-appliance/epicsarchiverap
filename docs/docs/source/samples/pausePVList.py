#!/usr/bin/env python3
'''Given a list of PVs, this pauses all the PVs in that list'''

import os
import sys
import argparse
import time
import requests
import json
import datetime
import time

def pausePVs(bplURL, pvNames):
    '''Pauses the pvs specified by the list pvNames'''
    url = bplURL + '/pauseArchivingPV'
    resp = requests.post(url, json=pvNames)
    resp.raise_for_status()
    pausePVResponse = resp.json()
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
        print("{0} => {1}".format(pvResponse['pvName'], pvResponse['status'] if 'status' in pvResponse else pvResponse['validation']))
