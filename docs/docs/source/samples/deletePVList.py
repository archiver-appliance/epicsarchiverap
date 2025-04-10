#!/usr/bin/env python3
'''Given a list of PVs, this deletes the PVs (including the data) in that list'''

import os
import sys
import argparse
import time
import requests
import json
import datetime
import time

def deletePV(bplURL, pvName):
    '''Deletes the pv specified by pvName'''
    url = bplURL + '/deletePV'
    params = {"pv": pvName, "deleteData": "true"}
    response = requests.get(url, params=params)
    response.raise_for_status()
    deletePVResponse = response.json()
    return deletePVResponse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Given a list of PVS in a files, this script deletes the PVs (including the data) for these PVs.")
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("file", help="A file with a list of PVS, one PV per line")
    args = parser.parse_args()
    lines = []
    with open(args.file, 'r') as f:
        lines = f.readlines()
    for line in lines:
        pvName = line.strip()
        deleteResponse = deletePV(args.url, pvName)
        print("{0} has been deleted with status {1}".format(pvName, deleteResponse['status'] if 'status' in deleteResponse else "N/A"))
        time.sleep(1.0)
    
