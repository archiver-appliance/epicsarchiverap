#!/usr/bin/env python
'''This script checks for the total number of disconnected PVs and sends and email out of the % of disconnected PVs exceeds MAX_DISCONN_PERCENTAGE %'''

import os
import sys
import argparse
import time
import urllib
import urllib2
import json
import datetime
import time
import emailHandler

# If the number of disconnected PVs is over this amount, we send an email out.
MAX_DISCONN_PERCENTAGE = 5.0

def getApplianceMetrics(bplURL):
    '''Get the appliance metrics'''
    url = bplURL + '/getApplianceMetrics'
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    the_page = response.read()
    applianceMetrics = json.loads(the_page)
    return applianceMetrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--disconnect_percentage", help="The amount of disconnected PVs we are willing to tolerate (in percentage)", default=MAX_DISCONN_PERCENTAGE, type=float)    
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")    
    args = parser.parse_args()
    if not args.url.endswith('bpl'):
        print "The URL needs to point to the mgmt bpl; for example, http://arch.slac.stanford.edu/mgmt/bpl. ", args.url
        sys.exit(1)

    disconnected_precentage = args.disconnect_percentage
    applianceMetrics = getApplianceMetrics(args.url)
    if applianceMetrics:
        emailMsg = "Disconnected PVs in {0}\n".format(args.url)
        sendEmail= False
        for applianceMetric in applianceMetrics:
            connectedPVs = int(applianceMetric['connectedPVCount'])
            disconnectedPVs = int(applianceMetric['disconnectedPVCount'])
            totalPVs = connectedPVs + disconnectedPVs
            if((disconnectedPVs*100.0)/totalPVs) > disconnected_precentage:
                sendEmail = True
                emailMsg = emailMsg + "{0} of {1} PVs in appliance {2} are in a disconnected state\n".format(disconnectedPVs, totalPVs, applianceMetric['instance'])
        if sendEmail:
            emailHandler.sendEmail("Disconnected PVs in " + args.url, emailMsg, [])
    else:
        print "Cannot obtain appliance metrics"
        emailHandler.sendEmail("checkConnectedPVs", "Cannot obtain appliance metrics from " + args.url, [])
