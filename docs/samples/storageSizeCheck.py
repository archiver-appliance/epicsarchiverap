#!/usr/bin/env python
'''Check the storage size report to make sure we do not have PV's archiving at more than the specified rate'''
import os
import sys
import argparse
import requests
import json
import datetime
import time
import operator

def getPVsWithEstimatedStorageGreaterThan(bplURL, limitInGbPerYear, limit):
    """
    Get a dict of PV to
    """
    resp = requests.get(bplURL + '/getStorageRateReport', params={"limit": limit})
    resp.raise_for_status()
    pv2s = { x["pvName"] : float(x["storageRate_GBperYear"]) for x in resp.json()}
    gpv2s = { k : v for k,v in pv2s.items() if v > limitInGbPerYear }
    return sorted(gpv2s.items(), key=operator.itemgetter(1), reverse=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("maxsize", help="Report any PV's archiving at a rate greater than this rate. This is specified in GB/year.", type=float)
    parser.add_argument("--limit", help="Limit the number of entries in the report.", type=int, default=100)
    args = parser.parse_args()
    bplURL = args.url + "/" if not args.url.endswith("/") else args.url
    for (pv, gbperyear)  in getPVsWithEstimatedStorageGreaterThan(bplURL, args.maxsize, args.limit):
        print("PV: {} Size(GB/year): {}".format(pv, gbperyear))
