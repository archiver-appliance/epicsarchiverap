#*******************************************************************************
# Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
# as Operator of the SLAC National Accelerator Laboratory.
# Copyright (c) 2011 Brookhaven National Laboratory.
# EPICS archiver appliance is distributed subject to a Software License Agreement found
# in file LICENSE that is included with this distribution.
#*******************************************************************************
#!/bin/bash

# Run this script in the same folder as the .proto files.
protoc --java_out=../../../../.. EPICSEvent.proto
