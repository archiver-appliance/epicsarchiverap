#!/usr/bin/env python3
''' Consolidate data for a PV '''

import argparse
import json
import requests
from time import sleep

def pausePV(bplURL, pvParams, debug=False):
    url = bplURL + '/pauseArchivingPV'
    resp = requests.get(url, params=pvParams)
    if debug: print('Url: {}'.format(resp.url))
    resp.raise_for_status()
    return resp.json() if (debug or 'status' not in resp.json())else resp.json()['status']

def resumePV(bplURL, pvParams, debug=False):
    url = bplURL + '/resumeArchivingPV'
    resp = requests.get(url, params=pvParams)
    if debug: print('Url: {}'.format(resp.url))
    resp.raise_for_status()
    return resp.json() if debug else resp.json()['status']

def getPVStatus(bplURL, pvParams, debug=False):
    url = bplURL + '/getPVStatus'
    resp = requests.get(url, params=pvParams)
    if debug: print('Url: {}'.format(resp.url))
    resp.raise_for_status()
    return resp.json() if debug else resp.json()[0]['status']

def consolidatePVData(bplURL, pvParams, debug=False):
    url = bplURL + '/consolidateDataForPV'
    resp = requests.get(url, params=pvParams)
    if debug: print('Url: {}'.format(resp.url))
    resp.raise_for_status()
    return resp.json() if debug else resp.json()['desc']

def doAll(bplURL, pvParams, store, debug=False):
    print('Pausing PV...')
    resp = pausePV(bplURL, pvParams, debug)
    print('Pause response: {}'.format(resp))
    sleep(0.25)
    print('Consolidating PV data...')
    resp = consolidatePVData(bplURL, {**pvParams, 'storage': store}, debug)
    print('Consolidate response: {}'.format(resp))
    sleep(0.25)
    print('Resuming PV...')
    resp = resumePV(bplURL, pvParams, debug)
    print('Resume response: {}'.format(resp))
    sleep(0.25)
    resp = getPVStatus(bplURL, pvParams, debug)
    print('PV status: {}\n'.format(resp))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredOne = parser.add_mutually_exclusive_group(required=True)
    requiredNamed.add_argument('-u', '--url', required=True,
            help='''URL of the mgmt bpl interface of the appliance cluster. 
            For example, http://arch.slac.stanford.edu/mgmt/bpl''')
    requiredNamed.add_argument('-s', '--store', help='Data store', required=True)
    requiredOne.add_argument('-p', '--pv', help='PV name')
    requiredOne.add_argument('-f', '--infile', help="A text file, each line containing a PV name")
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug info')
    args = parser.parse_args()

    if args.pv:
        pvParamDict = {'pv': args.pv}
        doAll(args.url, pvParamDict, args.store, args.debug)
    elif args.infile:
        with open(args.infile, 'r') as f:
            pvs = [line.strip() for line in f if not line.startswith('#')]
            pvs = [line for line in pvs if line]
            for pv in pvs:
                pvParamDict = {'pv': pv}
                doAll(args.url, pvParamDict, args.store, args.debug)
    else:
        print('Error: PV or filename required')
        parser.print_help()
        

