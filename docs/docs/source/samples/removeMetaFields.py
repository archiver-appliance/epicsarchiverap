#!/usr/bin/env python3

# This script demonstrates how to modify the extra fields (HIHI, LOLO etc) that are typically archived as part of a PV.
# These are set in a policy and can sometimes result in high casr connections on the IOC side.
# This script takes a PV name and removes all the extra fields.
# The PV's must be paused and resumed before calling this script.

import sys
import requests
import json
import argparse
import csv
import time

def getPVTypeInfo(bplURL, pvName):
	'''Gets the PV TypeInfo for the pv'''
	url = bplURL + '/getPVTypeInfo'
	response = requests.get(url, params={"pv" : pvName})
	response.raise_for_status()
	pvTypeInfo = response.json()
	return pvTypeInfo

def updatePVTypeInfo(bplURL, pvName, newTypeInfo):
	'''Updates the PV TypeInfo for the pv'''
	url = bplURL + '/putPVTypeInfo'
	params = { 'pv' : pvName, 'override' : 'true' }
	response = requests.post(url, params=params, json=newTypeInfo)
	response.raise_for_status()
	updatedTypeInfo = response.json()
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
		print("PV", pvName, "is currently archiving", typeInfo['archiveFields'])
		typeInfo['archiveFields'] = []
		typeInfo = updatePVTypeInfo(serverURL, pvName, typeInfo)
		time.sleep(0.25)
	else:
		print("PV", pvName, "is not currently archiving any metafields")

