#!/usr/bin/env python3
'''This script gets a list of PVs that are currently disconnected. Of these PVs, we stop archiving (and delete data) for those PVs from whom we did not receive a timestamp at all.
Be careful, this script has the ability to delete data.  
'''

import os
import sys
import argparse
import time
import requests
import urllib.parse
import json
import datetime

def getCurrentlyDisconnectedPVs(bplURL):
    '''Get a list of PVs that are currently disconnected'''
    url = bplURL + '/getCurrentlyDisconnectedPVs'
    response = requests.get(url)
    response.raise_for_status()
    currentlyDisconnectedPVs = response.json()
    return currentlyDisconnectedPVs

def pauseArchivingPV(bplURL, pv):
    '''Pauses the archiving of the PV.'''
    url = bplURL + '/pauseArchivingPV'
    params = {'pv' : pv}
    print("Pausing request for pv", pv, "using url", url + "?" + urllib.parse.urlencode(params))
    response = requests.get(url, params=params)
    response.raise_for_status()
    pauseResponse = response.json()
    return pauseResponse


def stopArchivingPV(bplURL, pv, deleteData):
    '''Stops archiving of a PV. If deleteData is true, we also delete the data associated with the PV.'''
    url = bplURL + '/deletePV'
    params = {'pv' : pv, 'deleteData' : 'true' if deleteData else 'false'}
    print("Aborting request for pv", pv, "using url", url + "?" + urllib.parse.urlencode(params))
    response = requests.get(url, params=params)
    response.raise_for_status()
    stopResponse = response.json()
    return stopResponse
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")    
    args = parser.parse_args()
    if not args.url.endswith('bpl'):
        print("The URL needs to point to the mgmt bpl; for example, http://arch.slac.stanford.edu/mgmt/bpl. ", args.url)
        sys.exit(1)
    currentlyDisconnectedPVs = getCurrentlyDisconnectedPVs(args.url)
    print(len(currentlyDisconnectedPVs), "PVs are currently disconnected")
    
    currentlyDisconnectedPVsWithUnknownLastKnowEvent = [x['pvName'] for x in currentlyDisconnectedPVs if x['lastKnownEvent'] == 'Never']
    print(len(currentlyDisconnectedPVsWithUnknownLastKnowEvent), "PVs are currently disconnected and have Never as the last known event.")
    
    for currentlyDisconnectedPVWithUnknownLastKnowEvent in currentlyDisconnectedPVsWithUnknownLastKnowEvent:
        pauseResponse = pauseArchivingPV(args.url, currentlyDisconnectedPVWithUnknownLastKnowEvent)
        time.sleep(1)
        stopResponse = stopArchivingPV(args.url, currentlyDisconnectedPVWithUnknownLastKnowEvent, True)
        print(stopResponse)
        time.sleep(1)
        
