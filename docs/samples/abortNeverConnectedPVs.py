#!/usr/bin/env python
'''This script gets a list of PVS that never connected and then aborts the archive request for these PVs'''

import os
import sys
import argparse
import datetime
import time
import json
import requests
import pytz
from dateutil.parser import parse as dateutilparse
from dateutil.tz import tzlocal

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("-o", "--older", help="Abort those PV's whose workflow started more that this many days ago. To abort all PV's, specify 0.", default=7, type=int)
    args = parser.parse_args()
    if not args.url.endswith('bpl'):
        print("The URL needs to point to the mgmt bpl; for example, http://arch.slac.stanford.edu/mgmt/bpl. ", args.url)
        sys.exit(1)
    neverConnectedPVs = requests.get(args.url + '/getNeverConnectedPVs').json()
    for neverConnectedPV in neverConnectedPVs:
        abort = False
        if args.older == 0:
            abort = True
        elif "startOfWorkflow" in neverConnectedPV:
            startOfWorkflow = dateutilparse(neverConnectedPV["startOfWorkflow"])
            if (datetime.datetime.now(tzlocal()) - startOfWorkflow).total_seconds() >= (args.older*86400):
                abort = True

        if abort:
            print("Aborting PV %s " % neverConnectedPV['pvName'])
            aresp = requests.get(args.url + '/abortArchivingPV', params={"pv": neverConnectedPV['pvName']})
            aresp.raise_for_status()
            time.sleep(0.25)
