#!/usr/bin/env python3
'''Find the list of paused PV's in the archiver and consolidate the data for these PV's all the way to the specified store'''

import os
import sys
import argparse
import json
import requests

def getPausedPVs(bplURL):
    '''Get the list of paused PV's'''
    resp = requests.get(bplURL + '/getPausedPVsReport')
    resp.raise_for_status()
    return [x["pvName"] for x in resp.json()]

def consolidateDataForPV(bplURL, storename, pvname):
    '''Consolidate the data for the specificed PV to the specified store'''
    resp = requests.get(bplURL + '/consolidateDataForPV', params={"pv": pvname, "storage": storename})
    resp.raise_for_status()
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("store", help="The name of the store to consolidate the data")
    args = parser.parse_args()
    pvnames = getPausedPVs(args.url)
    for pvname in pvnames:
        print("Consolidating data for ", pvname)
        consolidateDataForPV(args.url, args.store, pvname)
