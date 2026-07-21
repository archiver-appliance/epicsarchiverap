#!/usr/bin/env python3
'''Given a list of PVs matching  pattern, this resumes all the PVs in that list that are paused'''

import os
import sys
import argparse
import requests
import json
import datetime
import time

def resumePVs(bplURL, pvNames):
    '''Resumes the pvs specified by list pvNames'''
    url = bplURL + '/resumeArchivingPV'
    response = requests.post(url, json=pvNames)
    response.raise_for_status()
    resumePVResponse = response.json()
    return resumePVResponse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("pattern", help="A GLOB pattern")
    args = parser.parse_args()
    pvstat = requests.get(args.url + "/getPVStatus", params={"pv": args.pattern})
    pvstat.raise_for_status()
    pvs = pvstat.json()
    for pv in pvs:
        if pv.get("status", "") == "Paused":
            pvName = pv["pvName"]
            print("Resuming PV " + pvName)
            rsmstat = requests.get(args.url + "/resumeArchivingPV", params={"pv": pvName})
            rsmstat.raise_for_status()
    '''The for loop above can also be replaced with the resumePVs() function as follows'''
    # pvNames = [pv['pvName'] for pv in pvs if 'pvName' in pv]
    # resumePVs(args.url, pvNames)
