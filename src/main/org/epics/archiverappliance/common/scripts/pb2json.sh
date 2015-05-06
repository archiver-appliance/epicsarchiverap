#!/bin/bash

# This script prints the samples in the speficied PB files as JSON.

SCRIPT_FOLDER=`dirname $BASH_SOURCE`
WEBINF_FOLDER="$SCRIPT_FOLDER/../../WEB-INF"

if [[ ! -d "${WEBINF_FOLDER}/lib" || ! -d "${WEBINF_FOLDER}/classes" ]]
then
  echo "Unable to determine location of mgmt WEB-INF folder based on location of $0"
  exit -1
fi

CLASSPATH="${WEBINF_FOLDER}/classes"

pushd "${WEBINF_FOLDER}/lib" > /dev/null
for file in *.jar
do
	CLASSPATH="${CLASSPATH}:${WEBINF_FOLDER}/lib/${file}"
done
popd > /dev/null

java -Xmx2G -cp "${CLASSPATH}" edu.stanford.slac.archiverappliance.PlainPB.utils.PB2JSON $@

