#!/usr/bin/env python3
'''This script checks if any PVs changed type and then send mail if it detects any type changes'''

import os
import sys
import argparse
import time
import requests
import json
import datetime
import time
import emailHandler

def getTypeChangedPVs(bplURL):
    '''Get a list of PVs that changed type'''
    url = bplURL + '/getPVsByDroppedEventsTypeChange'
    response = requests.get(url)
    response.raise_for_status()
    typeChangedPVs = response.json()
    return typeChangedPVs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")    
    args = parser.parse_args()
    if not args.url.endswith('bpl'):
        print("The URL needs to point to the mgmt bpl; for example, http://arch.slac.stanford.edu/mgmt/bpl. ", args.url)
        sys.exit(1)
    typeChangedPVs = getTypeChangedPVs(args.url)
    if typeChangedPVs:
        print(len(typeChangedPVs), "PVs have changed type")
        emailHandler.sendEmail("PV Type changes", "Some PVs have changed type in " + args.url, [x['pvName'] for x in typeChangedPVs])
    else:
        print("No PVs have changed type")
