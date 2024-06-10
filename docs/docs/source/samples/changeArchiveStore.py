#!/usr/bin/env python3
''' Change storage parameters for a PV '''

import argparse
import json
import requests
from time import sleep


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

def getTypeInfo(bplURL, pvParams, debug=False):
    url = bplURL + '/getPVTypeInfo'
    resp = requests.get(url, params=pvParams)
    if debug: print('Url: {}'.format(resp.url))
    resp.raise_for_status()
    return resp.json()

def putTypeInfo(bplURL, pvParams, pvData, debug=False):
    url = bplURL + '/putPVTypeInfo'
    resp = requests.post(url, params=pvParams, json=pvData)
    if debug: print('Url: {}'.format(resp.url))
    resp.raise_for_status()
    return resp.status_code

def doAll(bplURL, pvParams, old=None, new=None, test=False, debug=False):
    resp = getTypeInfo(bplURL, pvParams)
    if debug:
        resp = getPVStatus(bplURL, pvParams)
        print('PV status: {}\n'.format(resp))
        print('\nCurrent PV type info: {}\n'.format(json.dumps(resp, indent=4, sort_keys=True)))
    if 'dataStores' in resp:
        pv_type_info = resp
        stores = pv_type_info['dataStores']
        print('Current data stores: {}\n'.format(json.dumps(stores, indent=4, sort_keys=True)))
        if old and new:
            new_stores = [x.replace(old, new) for x in stores]
            pv_type_info['dataStores'] = new_stores
            if debug or test:
                print('Proposed type info: {}\n'.format(json.dumps(pv_type_info, indent=4, sort_keys=True)))
            if not test:
                print('Setting new data store...')
                resp = putTypeInfo(bplURL, {**pvParams, 'override': 'true'}, pv_type_info)
                print('Response returned: {}\n'.format(resp))
                sleep(0.25)
                resp = getTypeInfo(bplURL, pvParams)
                print('New PV type info: {}\n'.format(json.dumps(resp, indent=4, sort_keys=True)))
                if debug:
                    resp = getPVStatus(bplURL, pvParams)
                    print('PV status: {}\n'.format(resp))
    else:
        print('dataStores not found in response')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredOne = parser.add_mutually_exclusive_group(required=True)
    requiredNamed.add_argument('-u', '--url', 
            help='''URL of the mgmt bpl interface of the appliance cluster. 
            For example, http://arch.slac.stanford.edu/mgmt/bpl''')
    requiredOne.add_argument('-p', '--pv', help='PV name')
    requiredOne.add_argument('-f', '--infile', help="A text file, each line containing a PV name")
    parser.add_argument('-o', '--old', help='Old data store, single-quoted')
    parser.add_argument('-n', '--new', help='New data store, single-quoted')
    parser.add_argument('-d', '--debug', action='store_true', help='Print debug info')
    parser.add_argument('-t', '--test', action='store_true', help="Test only, don't write new data")
    args = parser.parse_args()

    if ((args.old is not None and args.new is None) or (args.new is not None and args.old is None)):
        parser.error('Both old and new arguments required (or neither)')
        parser.print_help()

    if args.pv:
        pvParamDict = {'pv': args.pv}
        doAll(args.url, pvParamDict, old=args.old, new=args.new, test=args.test, debug=args.debug)
    elif args.infile:
        with open(args.infile, 'r') as f:
            pvs = [line.strip() for line in f if not line.startswith('#')]
            pvs = [line for line in pvs if line]
            for pv in pvs:
                pvParamDict = {'pv': pv}
                doAll(args.url, pvParamDict, old=args.old, new=args.new, test=args.test, debug=args.debug)
    else:
        parser.error('PV or filename required')
        parser.print_help()

