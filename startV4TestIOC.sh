#*******************************************************************************
# Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
# as Operator of the SLAC National Accelerator Laboratory.
# Copyright (c) 2011 Brookhaven National Laboratory.
# EPICS archiver appliance is distributed subject to a Software License Agreement found
# in file LICENSE that is included with this distribution.
#*******************************************************************************
#!/bin/bash

# This script sets up the classpath and such to run the benchmarks in this folder.
# It assumes that you are running from within the eclipse project folder

LOG4J_PROPERTIES=log4j.properties
while getopts ":v" Option
# We only have a verbose argument for now.
do
  case $Option in
    v ) LOG4J_PROPERTIES=log4j.properties.debug
  esac
done
shift $(($OPTIND - 1))

if [[ -z ${SCRIPTS_DIR} ]]
then 
  SCRIPTS_DIR=.
fi


CLASSPATH="${SCRIPTS_DIR}:${SCRIPTS_DIR}/bin:${SCRIPTS_DIR}/lib/protobuf-java-2.4.1.jar:${SCRIPTS_DIR}/lib/joda-time-2.0.jar:${SCRIPTS_DIR}/lib/jca-2.3.3.jar:${SCRIPTS_DIR}/lib/opencsv-2.3.jar:${SCRIPTS_DIR}/lib/log4j-1.2.16.jar:lib/guava-r09.jar:${SCRIPTS_DIR}/lib/jopt-simple-3.2-rc1.jar:${SCRIPTS_DIR}/lib/test/json-20080701.jar:${SCRIPTS_DIR}/lib/test/pvIOC-1.0-BETA.jar:${SCRIPTS_DIR}/lib/test/pvData-1.0-BETA.jar:${SCRIPTS_DIR}/lib/test/pvAccess-1.0-BETA.jar"
echo ${CLASSPATH}

java -Xmx2G -Xms2G -Dlog4j.configuration=${LOG4J_PROPERTIES} -classpath ${CLASSPATH} $@ org.epics.archiverappliance.engine.epicsv4.StartIOC4EPICSV4


