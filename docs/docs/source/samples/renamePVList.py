#!/usr/bin/env python
'''Given a list of comma separated PVs, this renames  all the PVs in the first column to the second column.
We assume that the PV's have been paused etc.
For example, generate a list like so
OldName0, NewName0
OldName1, NewName1

To actually do this
1) Pause all the old names using the pausePVList script
2) Use this script to rename the oldName to newName
3) Resume the newNames using the resumePVList script
4) Delete the oldNames (including the data) using the deletePVList script
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

def renamePV(bplURL, oldName, newName):
    '''Renames the pv oldName to newName'''
    url = bplURL + '/renamePV?pv=' + urllib.quote_plus(oldName) + "&newname=" + urllib.quote_plus(newName)
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    the_page = response.read()
    renamePVResponse = json.loads(the_page)
    return renamePVResponse



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="This is the URL to the mgmt bpl interface of the appliance cluster. For example, http://arch.slac.stanford.edu/mgmt/bpl")
    parser.add_argument("file", help="A file with a list of PVS, two PVs per line separated by a comma")
    args = parser.parse_args()
    lines = []
    with open(args.file, 'r') as f:
        lines = f.readlines()
    for line in lines:
        line = line.strip()
        parts = line.split(",")
        if len(parts) != 2:
            print "Skipping line", line
            continue
        oldName = parts[0]
        newName = parts[1]
        renameResponse = renamePV(args.url, oldName, newName)
        print "{0} has been renamed to {1} with status {2}".format(oldName, newName, renameResponse['status'] if 'status' in renameResponse else "N/A")
        time.sleep(1.0)
    