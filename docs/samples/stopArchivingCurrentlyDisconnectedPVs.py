#!/usr/bin/env python
'''This script gets a list of PVs that are currently disconnected. Of these PVs, we stop archiving (and delete data) for those PVs from whom we did not receive a timestamp at all.
Be careful, this script has the ability to delete data.  
'''

import os
import sys
import argparse
import time
import urllib
import urllib2
import json
import datetime
import time

def getCurrentlyDisconnectedPVs(bplURL):
    '''Get a list of PVs that are currently disconnected'''
    url = bplURL + '/getCurrentlyDisconnectedPVs'
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    the_page = response.read()
    currentlyDisconnectedPVs = json.loads(the_page)
    return currentlyDisconnectedPVs

def pauseArchivingPV(bplURL, pv):
    '''Pauses the archiving of the PV.'''
    values = url_values = urllib.urlencode({'pv' : pv})
    url = bplURL + '/pauseArchivingPV?' + values
    print "Pausing request for pv", pv, "using url", url
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    the_page = response.read()
    pauseResponse = json.loads(the_page)
    return pauseResponse


def stopArchivingPV(bplURL, pv, deleteData):
    '''Stops archiving of a PV. If deleteData is true, we also delete the data associated with the PV.'''
    values = url_values = urllib.urlencode({'pv' : pv, 'deleteData' : 'true' if deleteData else 'false'})
    url = bplURL + '/deletePV?' + values
    print "Aborting request for pv", pv, "using url", url
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    the_page = response.read()
    stopResponse = json.loads(the_page)
    return stopResponse
    


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")    
    args = parser.parse_args()
    if not args.url.endswith('bpl'):
        print "The URL needs to point to the mgmt bpl; for example, http://arch.slac.stanford.edu/mgmt/bpl. ", args.url
        sys.exit(1)
    currentlyDisconnectedPVs = getCurrentlyDisconnectedPVs(args.url)
    print len(currentlyDisconnectedPVs), "PVs are currently disconnected"
    
    currentlyDisconnectedPVsWithUnknownLastKnowEvent = [x['pvName'] for x in currentlyDisconnectedPVs if x['lastKnownEvent'] == 'Never']
    print len(currentlyDisconnectedPVs), "PVs are currently disconnected and have Never as the last known event."
    
    for currentlyDisconnectedPVWithUnknownLastKnowEvent in currentlyDisconnectedPVsWithUnknownLastKnowEvent:
        pauseResponse = pauseArchivingPV(args.url, currentlyDisconnectedPVWithUnknownLastKnowEvent)
        time.sleep(1)
        stopResponse = stopArchivingPV(args.url, currentlyDisconnectedPVWithUnknownLastKnowEvent, True)
        print stopResponse
        time.sleep(1)
        
