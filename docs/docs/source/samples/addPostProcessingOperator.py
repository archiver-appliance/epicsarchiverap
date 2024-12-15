#!/usr/bin/env python3

# This script demonstrates how to change PVTypeInfo from python and uses as an example the use case of adding a post processing operator to a store named LTS
# We take any number of arguments (which can be wildcards) 
# We get PVs that match these arguments.
# We determine the data store that has a name=LTS argument. 
# We append a pp=mean_3600 post processing operator if it does not exist.

import sys
import requests
import json
import argparse
import csv
import urllib.parse

# Set up argparse and parse command line options
parser = argparse.ArgumentParser()
parser.add_argument('serverURL', help="URL to the EPICS archiver appliance mgmt webapp", nargs=1)
parser.add_argument('pvNames', help="One or more pvnames; these can be GLOB wildcards", nargs='+')

args = parser.parse_args()

serverURL = args.serverURL[0]
pvNames = args.pvNames

pvNamesCSV = ",".join(pvNames)
values = { 'pv' : pvNamesCSV }

resp = requests.get(f"{serverURL}/bpl/getAllPVs", params=values)
resp.raise_for_status()
matchingPVs = resp.json()

for pv in matchingPVs:
	print(pv)
	values = { 'pv' : pv }
	resp = requests.get(f"{serverURL}/bpl/getPVTypeInfo", params=values)
	resp.raise_for_status()
	typeInfo = resp.json()
	if typeInfo: 
		dataStores = typeInfo['dataStores']
		for index, dataStore in enumerate(dataStores):
			dataStoreQS = urllib.parse.urlparse(dataStore).query
			qsargs = urllib.parse.parse_qs(dataStoreQS)
			if qsargs['name'][0] == 'LTS':
				if 'pp' not in qsargs or 'mean_3600' not in qsargs['pp']:
					modifiedDataStore = dataStore + '&pp=mean_3600'
					print(modifiedDataStore)
					dataStores[index] = modifiedDataStore
					json_data = typeInfo
					headers = {"Content-type": "application/json", "Accept": "text/plain"}
					values = { 'pv' : pv, 'override' : 'true' }
					resp = requests.post(f"{serverURL}/bpl/putPVTypeInfo", params=values, json=json_data, headers=headers)
					resp.raise_for_status()
					updatedTypeInfo = resp.json()
					print(updatedTypeInfo['dataStores'])
				else:
					print('mean_3600 already exists as a post processing operator for PV ', pv)
