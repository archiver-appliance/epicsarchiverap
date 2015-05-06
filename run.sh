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


CLASSPATH="${SCRIPTS_DIR}:${SCRIPTS_DIR}/bin"
pushd ${SCRIPTS_DIR}/lib
for file in *.jar
do
	CLASSPATH=${CLASSPATH}:${SCRIPTS_DIR}/lib/${file}
done
popd


echo "Classpath is ${CLASSPATH}"

java -Xmx2G -Xms2G -Dlog4j.configuration=${LOG4J_PROPERTIES} -classpath ${CLASSPATH} $@


