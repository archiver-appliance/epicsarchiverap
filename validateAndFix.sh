#!/bin/bash

# Runs ValidateAndFixPBFile on all the args.

export SCRIPTS_DIR=`dirname $0`
if [[ ! -f ${SCRIPTS_DIR}/run.sh ]]
then
  echo "Unable to determine location of run.sh"
  exit
fi

${SCRIPTS_DIR}/run.sh edu.stanford.slac.archiverappliance.PlainPB.utils.ValidateAndFixPBFile $@

