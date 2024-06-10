#!/usr/bin/env python
'''Get all the PV's. For each PV, get the PV details and then sum up the ETL's for each PV by destination
In the PV details, we look for 
ETL 0 total time performing ETL(ms)
ETL 1 total time performing ETL(ms)
ETL 0 number of times we performed ETL
ETL 1 number of times we performed ETL
ETL 0 last job took (ms)
ETL 1 last job took (ms)
'''


import urllib
import urllib2
import json
import sys
import argparse

def getAllPVs(url):
    ''' Get all the PV's'''
    finalPVList = []
    applianceMGMTUrl = url + '/getAllPVs'
    resp = urllib2.urlopen(url=applianceMGMTUrl)
    matchingPVs = json.load(resp)
    finalPVList.extend(matchingPVs)
    return sorted(finalPVList)

def getPVDetails(url, pvName):
    try:
        applianceMGMTUrl = url + '/getPVDetails?pv=' + urllib.quote_plus(pvName)
        resp = urllib2.urlopen(url=applianceMGMTUrl)
        pvDetails = json.load(resp)
        return pvDetails
    except:
        print "Exception getting details for PV ", pv
        return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    args = parser.parse_args()
    pvs = getAllPVs(args.url)
    etl0 = 0
    etl1 = 0
    etl0avgofaverages = 0.0
    etl1avgofaverages = 0.0
    etl0lastJob = 0
    etl1lastJob = 0
    for pv in pvs:
        pvDetails = getPVDetails(args.url, pv)
        pvetl0 = 0
        pvetl1 = 0
        for pvDetail in pvDetails:
            if pvDetail['name'] == 'ETL 0 total time performing ETL(ms)':
                pvetl0 = int(pvDetail['value'])
                etl0 = etl0 + pvetl0 
            if pvDetail['name'] == 'ETL 1 total time performing ETL(ms)':
                pvetl1 = int(pvDetail['value'])
                etl1 = etl1 + pvetl1
            if pvDetail['name'] == 'ETL 0 last job took (ms)':
                pvetl0lastjob = int(pvDetail['value'])
                etl0lastJob = etl0lastJob + pvetl0lastjob
            if pvDetail['name'] == 'ETL 1 last job took (ms)':
                pvetl1lastjob = int(pvDetail['value'])
                etl1lastJob = etl1lastJob + pvetl1lastjob
            if pvDetail['name'] == 'ETL 0 number of times we performed ETL':
                if pvDetail['value'] != 'None so far':
                    etl0count = int(pvDetail['value'])
                    if etl0count != 0:
                        etl0avgofaverages = etl0avgofaverages + pvetl0/etl0count
            if pvDetail['name'] == 'ETL 1 number of times we performed ETL':
                if pvDetail['value'] != 'None so far':
                    etl1count = int(pvDetail['value'])
                    if etl1count != 0:
                        etl1avgofaverages = etl1avgofaverages + pvetl1/etl1count
    print "Total ETL(0) = {0}".format(etl0)
    print "Total ETL(1) = {0}".format(etl1)    
    print "Total ETL(0) average of averages = {0}".format(etl0avgofaverages)
    print "Total ETL(1) average of averages = {0}".format(etl1avgofaverages)    
    print "ETL 0 last job = {0}".format(etl0lastJob)
    print "ETL 1 last job = {0}".format(etl1lastJob)    
    