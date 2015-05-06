#!/usr/bin/env python

# This script demonstrates how to change PVTypeInfo from python and uses as an example the use case of adding a post processing operator to a store named LTS
# We take any number of arguments (which can be wildcards) 
# We get PVs that match these arguments.
# We determine the data store that has a name=LTS argument. 
# We append a pp=mean_3600 post processing operator if it does not exist.

import sys
import urllib
import urllib2
import json
import argparse
import csv
import urlparse

# Set up argparse and parse command line options
parser = argparse.ArgumentParser(description='Process EPICS IOC .db files for info/archiver tags, parse these and convert them to calls to an EPICS archiver appliance.')
parser.add_argument('serverURL', help="URL to the EPICS archiver appliance mgmt webapp", nargs=1)
parser.add_argument('pvNames', help="One or more pvnames; these can be GLOB wildcards", nargs='+')

args = parser.parse_args()

serverURL = args.serverURL[0]
pvNames = args.pvNames

pvNamesCSV = ",".join(pvNames)
values = { 'pv' : pvNamesCSV }
queryString = urllib.urlencode(values)

matchingPVs = json.load(urllib2.urlopen(serverURL + '/bpl/getAllPVs?' + queryString))

for pv in matchingPVs:
	print pv
	values = { 'pv' : pv }
	queryString = urllib.urlencode(values)
	typeInfo = json.load(urllib2.urlopen(serverURL + '/bpl/getPVTypeInfo?' + queryString))
	if typeInfo: 
		dataStores = typeInfo['dataStores']
		for index, dataStore in enumerate(dataStores):
		# urlparse wants to put the querystring into the path with the ? also present.
			dataStoreQS = urlparse.urlparse(dataStore).path.replace("?", "")
			qsargs = urlparse.parse_qs(dataStoreQS)
			if qsargs['name'][0] == 'LTS':
				if 'pp' not in qsargs or 'mean_3600' not in qsargs['pp']:
					modifiedDataStore = dataStore + '&pp=mean_3600'
					print modifiedDataStore
					dataStores[index] = modifiedDataStore
					data = json.dumps(typeInfo)
					headers = {"Content-type": "application/json", "Accept": "text/plain"}
					req = urllib2.Request(serverURL + '/bpl/putPVTypeInfo?' + queryString, data, headers)
					updatedTypeInfo = json.load(urllib2.urlopen(req))
					print updatedTypeInfo['dataStores']
				else:
					print 'mean_3600 already exists as a post processing operator for PV ', pv