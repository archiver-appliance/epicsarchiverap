#!/usr/bin/env python3
'''This script gets a list of PVS that are currently disconnected and then pings them using cainfo'''

import os
import sys
import argparse
import time
import requests
import json
import datetime
import time
import epics

def getCurrentlyDisconnectedPVs(bplURL):
    '''Get a list of PVs that are currently disconnected'''
    url = bplURL + '/getCurrentlyDisconnectedPVs'
    resp = requests.get(url)
    resp.raise_for_status()
    currentlyDisconnectedPVs = resp.json()
    return currentlyDisconnectedPVs

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    args = parser.parse_args()
    if not args.url.endswith('bpl'):
        print("The URL needs to point to the mgmt bpl; for example, http://arch.slac.stanford.edu/mgmt/bpl. ", args.url)
        sys.exit(1)
    currentlyDisconnectedPVs = getCurrentlyDisconnectedPVs(args.url)
    if not currentlyDisconnectedPVs:
        print("All PVs seems to be connected")
        sys.exit(0)
    pingablePVCount = 0 
    for currentlyDisconnectedPV in currentlyDisconnectedPVs:
        pvName = currentlyDisconnectedPV['pvName']
        pvinfo = epics.cainfo(pvName, print_out=False)
        if pvinfo:
            print("PV ", pvName, "seems to be connectable using the current environment") 
            pingablePVCount = pingablePVCount + 1
    sys.exit(pingablePVCount)
