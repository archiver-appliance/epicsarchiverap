#!/bin/bash

# This script prints the record processing time stamps of the samples in one or more PB files.

SCRIPT_FOLDER=`dirname $BASH_SOURCE`
WEBINF_FOLDER="$SCRIPT_FOLDER/../../WEB-INF"

if [[ ! -d "${WEBINF_FOLDER}/lib" || ! -d "${WEBINF_FOLDER}/classes" ]]
then
  echo "Unable to determine location of mgmt WEB-INF folder based on location of $0"
  exit -1
fi

CLASSPATH="${WEBINF_FOLDER}/classes"

pushd "${WEBINF_FOLDER}/lib"
for file in *.jar
do
	CLASSPATH="${CLASSPATH}:${WEBINF_FOLDER}/lib/${file}"
done
popd

java -Xmx2G -cp "${CLASSPATH}" edu.stanford.slac.archiverappliance.PlainPB.utils.PrintTimes $@

