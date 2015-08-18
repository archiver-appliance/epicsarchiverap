#!/usr/bin/env python

import urllib
import urllib2
import json
import sys

PVPatterns = []

if(len(sys.argv) > 1):
	PVPatterns.extend(sys.argv[1:])

finalPVList = []
applianceMGMTUrl = 'http://archiver.slac.stanford.edu:17665/mgmt/bpl/getAllPVs'
if len(PVPatterns) > 0:
	for pattern in PVPatterns:
		resp = urllib2.urlopen(url=applianceMGMTUrl + "?" + urllib.urlencode({"pv" : pattern}))
		matchingPVs = json.load(resp)
		finalPVList.extend(matchingPVs)
else:
		resp = urllib2.urlopen(url=applianceMGMTUrl)
		matchingPVs = json.load(resp)
		finalPVList.extend(matchingPVs)
	

for pv in sorted(finalPVList):
	print pv

