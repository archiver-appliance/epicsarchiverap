#!/usr/bin/env python3
'''Get a list of PV's that have changed type and then display the previous/new type from the PV details'''
import os
import sys
import argparse
import requests
import json
import datetime
import time

def getTypesFromPVDetails(bplURL, pvName):
    '''Returns the previous and new type from the pv details'''
    resp = requests.get(bplURL + '/getPVDetails', params={"pv": pvName})
    resp.raise_for_status()
    pvdetails = { x["name"] : x["value"] for x in resp.json() }
    return pvdetails["Archiver DBR type (initial)"], pvdetails["Archiver DBR type (from CA)"]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    args = parser.parse_args()
    bplURL = args.url + "/" if not args.url.endswith("/") else args.url
    resp = requests.get(bplURL + "getPVsByDroppedEventsTypeChange")
    resp.raise_for_status()
    pvs = [ x["pvName"] for x in resp.json() ]
    for pv in pvs:
        prev, curr = getTypesFromPVDetails(bplURL, pv)
        print(f'PV: {pv:<40} Previous {prev:20} Current {curr:20}')
