#!/usr/bin/env python

# This is a simple script that demonstrates how to specify archiving parameters in the IOC's .db file and then use this information to add PV's to the EPICS archiver appliance.
# The usecase for this is as follows
# 1 IOC engineer adds info tags to his PV's like so 
# 1.1 'info(archiver, "scan;10")'
# 1.2 'info(archiver, "")'
# 1.3 'info(archiver, "monitor;10")'
# 2 IOC engineer runs a variation of this script when pushing the IOC to production
# This use case does not cater to cases like deleting PV's from the archiver, changing PV names and so on.
# However, this example serves to illustrate the basic form of integration.
#
# We use the Static Database Access routines provided by EPICS base in the libdbStaticHost.so to parse the IOC .db file
# This requires us to process the .dbd files as well; so you need to explicitly pass those in or include them in your .db file and set the paths accordingly.
#
# For example - ./archiveFromDB.py http://archiver.jdoe.edu/mgmt ../dbd/softIoc.dbd ../db/arch.d

import sys
import urllib
import urllib2
import json
import argparse
from ctypes import *

# Begin code where we use ctypes to load the functions we need from the .so

cdll.LoadLibrary("libdbStaticHost.so")
libdbparse = CDLL("libdbStaticHost.so") 

class DBBASE(Structure):
    pass

class DBENTRY(Structure):
    pass

DBBASEPTR = POINTER(DBBASE)
DBBASEPTRPTR = POINTER(DBBASEPTR)
DBENTRYPTR = POINTER(DBENTRY)

dbAllocBase = libdbparse.dbAllocBase
dbAllocBase.restype = DBBASEPTR

dbFreeBase = libdbparse.dbFreeBase
dbFreeBase.restype = None

dbReadDatabase = libdbparse.dbReadDatabase
dbReadDatabase.restype = c_int
dbReadDatabase.argtypes = [DBBASEPTRPTR, c_char_p, c_char_p, c_char_p]

dbAllocEntry = libdbparse.dbAllocEntry
dbAllocEntry.restype = DBENTRYPTR
dbAllocEntry.argtypes = [DBBASEPTR]

dbFreeEntry = libdbparse.dbFreeEntry
dbFreeEntry.restype = None
dbFreeEntry.argtypes = [DBENTRYPTR]

dbFirstRecordType = libdbparse.dbFirstRecordType
dbFirstRecordType.restype = c_int
dbFirstRecordType.argtypes = [DBENTRYPTR]

dbNextRecordType = libdbparse.dbNextRecordType
dbNextRecordType.restype = c_int
dbNextRecordType.argtypes = [DBENTRYPTR]

dbGetRecordTypeName = libdbparse.dbGetRecordTypeName
dbGetRecordTypeName.restype = c_char_p
dbGetRecordTypeName.argtypes = [DBENTRYPTR]

dbFirstRecord = libdbparse.dbFirstRecord
dbFirstRecord.argtypes = [DBENTRYPTR]

dbNextRecord = libdbparse.dbNextRecord
dbNextRecord.argtypes = [DBENTRYPTR]

dbGetRecordName = libdbparse.dbGetRecordName
dbGetRecordName.restype = c_char_p
dbGetRecordName.argtypes = [DBENTRYPTR]

dbIsAlias = libdbparse.dbIsAlias
dbIsAlias.restype = c_int
dbIsAlias.argtypes = [DBENTRYPTR]

dbFirstField = libdbparse.dbFirstField
dbFirstField.restype = c_int
dbFirstField.argtypes = [DBENTRYPTR, c_int]

dbNextField = libdbparse.dbNextField
dbNextField.restype = c_int
dbNextField.argtypes = [DBENTRYPTR, c_int]

dbGetFieldName = libdbparse.dbGetFieldName
dbGetFieldName.restype = c_char_p
dbGetFieldName.argtypes = [DBENTRYPTR]

dbFirstInfo = libdbparse.dbFirstInfo
dbFirstInfo.restype = c_int
dbFirstInfo.argtypes = [DBENTRYPTR]

dbNextInfo = libdbparse.dbNextInfo
dbNextInfo.restype = c_int
dbNextInfo.argtypes = [DBENTRYPTR]

dbGetInfoName = libdbparse.dbGetInfoName
dbGetInfoName.restype = c_char_p
dbGetInfoName.argtypes = [DBENTRYPTR]

dbGetInfo = libdbparse.dbGetInfo
dbGetInfo.restype = c_char_p
dbGetInfo.argtypes = [DBENTRYPTR, c_char_p]

# End code where we use ctypes to load the functions we need from the .so


# Set up argparse and parse command line options
parser = argparse.ArgumentParser(description='Process EPICS IOC .db files for info/archiver tags, parse these and convert them to calls to an EPICS archiver appliance.')
parser.add_argument('-m', help="Pass macros in the format macro=value,macro2=value2", nargs=1)
parser.add_argument('-p', help="Path to use for all the include statements in the db file", nargs=1)
parser.add_argument('serverURL', help="URL to the EPICS archiver appliance mgmt webapp", nargs=1)
parser.add_argument('dbFiles', help="One or more db files to process", nargs='+')

args = parser.parse_args()

serverURL = args.serverURL[0]
dbFiles = args.dbFiles
macros = ''
path = ''
if args.m:
	macros = args.m
if args.p:
	path = args.p

# Done setting up argparse and parse command line options

# Read all the db files
archivepvlist = []
db = dbAllocBase()
for dbfile in dbFiles:
	status = dbReadDatabase(pointer(db), dbfile, path, macros)
	if status > 0:
	    print "Could not read database " + dbfile
	    sys.exit(1)
	
entry = dbAllocEntry(db)

status = dbFirstRecordType(entry)
if status > 0:
    print "No record descriptions"
    sys.exit(1)
while status == 0:
    recordTypeName = dbGetRecordTypeName(entry)
    status = dbFirstRecord(entry)
    while status == 0:
        recordName = dbGetRecordName(entry)
        value = dbGetInfo(entry, "archiver")
        if value != None:
            archparams = ""
            if ';' in value:
                archparams = value.split(";")
                archparmsdict = {}
                archparmsdict['pv'] = recordName
                archparmsdict['samplingmethod'] = archparams[0]
                archparmsdict['samplingperiod'] = archparams[1]
                archivepvlist.append(archparmsdict)
            else:
                archparmsdict = {}
                archparmsdict['pv'] = recordName
                archivepvlist.append(archparmsdict)
        status = dbNextRecord(entry)
    status = dbNextRecordType(entry)

if archivepvlist:
    data = json.dumps(archivepvlist)
    url = serverURL + '/bpl/archivePV'
    headers = {"Content-type": "application/json", "Accept": "text/plain"}
    print "Submitting PVs to appliance archiver at ", url
    req = urllib2.Request(url, data, headers)
    response = urllib2.urlopen(req)
    the_page = response.read()
    status = json.loads(the_page)
    for pvstatus in status:
        print pvstatus['pvName'], pvstatus['status']

#dbFreeBase(db)
#dbFreeEntry(entry)


