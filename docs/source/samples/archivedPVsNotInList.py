#!/usr/bin/env python3
'''Given a list of comma separated PVs, determine all the PV's that are being archived that are not in this list.'''

import argparse
import requests
import json

def getUnarchivedPVs(bplURL, pvNames):
    '''Of the PVs in the list, determine those that are unarchived. '''
    url = bplURL + '/archivedPVsNotInList'
    data = {"pv": ",".join(pvNames)}
    response = requests.post(url, data=data)
    response.raise_for_status()
    retval = response.json()
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
        pvNames.append(pvName)
    unarchivedPVs = getUnarchivedPVs(args.url, pvNames)
    for unarchivedPV in sorted(unarchivedPVs):
        print(unarchivedPV)
        
        
    
