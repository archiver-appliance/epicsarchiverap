#!/usr/bin/env python3

import requests
import json
import sys

PVPatterns = []

if(len(sys.argv) > 1):
	PVPatterns.extend(sys.argv[1:])

finalPVList = []
applianceMGMTUrl = 'http://archiver.slac.stanford.edu:17665/mgmt/bpl/getAllPVs'
if len(PVPatterns) > 0:
	for pattern in PVPatterns:
		resp = requests.get(applianceMGMTUrl, params={"pv" : pattern})
		resp.raise_for_status()
		matchingPVs = resp.json()
		finalPVList.extend(matchingPVs)
else:
		resp = requests.get(applianceMGMTUrl)
		resp.raise_for_status()
		matchingPVs = resp.json()
		finalPVList.extend(matchingPVs)
	

for pv in sorted(finalPVList):
	print(pv)

