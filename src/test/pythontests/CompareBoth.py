#!/usr/bin/env python

import sys
import os
import time
from datetime import datetime
import urllib
import urllib2
import json
import xmlrpclib


secondsToGetData = 1000

def fetchJSON(archApplURL, pvName, startTime, endTime):
	params = {}
	params['pv'] = pvName
	params['from'] = datetime.fromtimestamp(startTime).strftime("%Y-%m-%dT%H:%M:%S.000-07:00")
	params['to'] = datetime.fromtimestamp(endTime).strftime("%Y-%m-%dT%H:%M:%S.000-07:00")
	params['donotchunk'] = 'true'
	retrievalURL = archApplURL + "/getData.json?"+ urllib.urlencode(params);
	print "Asking for data from ",  retrievalURL
	req = urllib2.urlopen(retrievalURL)
	data = json.load(req)
	return data[0]['data']

def fetchCAData(xmlRPCURL, pvName, startTime, endTime):
	print "Asking for data from ",  xmlRPCURL
	proxy =  xmlrpclib.ServerProxy(xmlRPCURL)
	xmldata = proxy.archiver.values(1, [pvName], startTime, 0, endTime, 0, secondsToGetData, 0)
	return xmldata[0]['values']


def compareEvents(archApplURL, xmlRPCURL, pvName, startTime, endTime):
	print archApplURL, xmlRPCURL, pvName
	jsonData = fetchJSON(archApplURL, pvName, startTime, endTime)
	print "Event from appliance = {0}".format(len(jsonData))
	caData = fetchCAData(xmlRPCURL, pvName, startTime, endTime)
	print "Event from CA = {0}".format(len(caData))
	jsonevnum = 0
	caevnum = 0
	while jsonevnum < len(jsonData) and caevnum < len(caData):
		if jsonevnum < len(jsonData):			
			jsonEvent = jsonData[jsonevnum]
		if caevnum < len(caData):
			caEvent = caData[caevnum]
		if jsonEvent and caEvent:
			if jsonEvent['secs'] > caEvent['secs']:
				caevnum = caevnum + 1
				print "\t\t\t\t{0}".format(caEvent['secs'])
			elif jsonEvent['secs'] < caEvent['secs']:
				jsonevnum = jsonevnum + 1
				print "{0}".format(jsonEvent['secs'])
			else:
				jsonevnum = jsonevnum + 1
				caevnum = caevnum + 1
				jsval = round(float(jsonEvent['val']),2)
				caval = round(float(caEvent['value'][0]),2)  
				if jsval != caval:
					print "Values are different JS:{0} and CA{1}".format(jsval, caval)

if __name__ == "__main__":
	if len(sys.argv) < 4:
		print "Usage: ", sys.argv[0], "<ArchApplURL> <XMLRPCUrl> <PV>"
		sys.exit(1)
	archApplURL = sys.argv[1]
	xmlRPCURL = sys.argv[2]
	pvName = sys.argv[3]
	endTime = int(time.time())
	startTime = endTime - secondsToGetData
	compareEvents(archApplURL, xmlRPCURL, pvName, startTime, endTime)


