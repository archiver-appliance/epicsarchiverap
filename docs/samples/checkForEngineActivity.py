#!/usr/bin/env python

# This script demonstrates how to check for recent engine activity if using PB files on the file system as the storage plugin.
# It walks the entire folder tree and generates a snapshot. It then sleeps for some time and does this again.
# It compares sizes with last known sizes. 
# Typically the engine writes to stores that have fast IOPS so this kinda brute force approach should perform reasonably.
# Most commercial monitoring tools have more intelligent/better performing APIs to handle this. 

import os
import sys
import sets
import time
import argparse

def generatePathSizeTree(folder):
	pathSizeTree = sets.Set()
	for root, dirs, files in os.walk(folder):
		for name in files:
			fullPath = os.path.join(root, name)
			pathSize = "{0}_{1}".format(fullPath, os.path.getsize(fullPath))
			pathSizeTree.add(pathSize)
	return pathSizeTree


# Set up argparse and parse command line options
parser = argparse.ArgumentParser(description='Check for EPICS archiver appliance engine activity in a folder.')
parser.add_argument('folder', help="The path to the folder storing the PB files", nargs=1)
parser.add_argument('-t', help="Specify the timeout to wait between folder walks", nargs=1, default=30)

args = parser.parse_args()

STSfolder = args.folder[0]
timeout = args.t

before = generatePathSizeTree(STSfolder)
time.sleep(timeout)
after = generatePathSizeTree(STSfolder)

filesWithChanges = after.difference(before)
if filesWithChanges:
	print len(filesWithChanges), " changes were detected in ", timeout, " seconds"
else:
	print "No changes detected in the last ", timeout, " seconds"
	sys.exit(-1)

