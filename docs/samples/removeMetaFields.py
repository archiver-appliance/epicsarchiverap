#!/usr/bin/env python

# This script demonstrates how to modify the extra fields (HIHI, LOLO etc) that are typically archived as part of a PV.
# These are set in a policy and can sometimes result in high casr connections on the IOC side.
# This script takes a PV name and removes all the extra fields.
# The PV's must be paused and resumed before calling this script.

import sys
import urllib
import urllib2
import json
import argparse
import csv
import urlparse
import time

def getPVTypeInfo(bplURL, pvName):
	'''Gets the PV TypeInfo for the pv'''
	url = bplURL + '/getPVTypeInfo?pv=' + urllib.quote_plus(pvName)
	req = urllib2.Request(url)
	response = urllib2.urlopen(req)
	the_page = response.read()
	pvTypeInfo = json.loads(the_page)
	return pvTypeInfo

def updatePVTypeInfo(bplURL, pvName, newTypeInfo):
	'''Gets the PV TypeInfo for the pv'''
	url = bplURL + '/putPVTypeInfo?pv=' + urllib.quote_plus(pvName) + "&override=true"
	headers = {"Content-type": "application/json", "Accept": "text/plain"}
	newTypeInfoStr = json.dumps(newTypeInfo)
	req = urllib2.Request(url, data=newTypeInfoStr, headers=headers)
	response = urllib2.urlopen(req)
	updatedTypeInfo = json.load(response)
	return updatedTypeInfo


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Remove the metafields for the specified PV')
	parser.add_argument('serverURL', help="URL to the EPICS archiver appliance mgmt webapp")
	parser.add_argument('pvName', help="A pv for which to remove the meta fields")
	
	args = parser.parse_args()
	serverURL = args.serverURL
	pvName = args.pvName
	
	typeInfo = getPVTypeInfo(serverURL, pvName)
	time.sleep(0.25)
	if len(typeInfo['archiveFields']) > 0:
		print "PV", pvName, "is currently archiving", typeInfo['archiveFields']
		typeInfo['archiveFields'] = []
		typeInfo = updatePVTypeInfo(serverURL, pvName, typeInfo)
		time.sleep(0.25)
	else:
		print "PV", pvName, "is not currently archiving any metafields"

