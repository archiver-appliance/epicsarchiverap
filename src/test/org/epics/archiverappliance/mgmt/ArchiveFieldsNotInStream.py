#!/usr/bin/python

# policies.py for testing archive fileds not in stream


import sys
import os

# Generate a list of policy names. This is used to feed the dropdown in the UI.
def getPolicyList():
	pvPoliciesDict = {}
	pvPoliciesDict['Default'] = 'The default policy'
	return pvPoliciesDict

# Define a list of fields that will be archived as part of every PV.
def getFieldsArchivedAsPartOfStream():
	 return ['HIHI','HIGH','LOW','LOLO','LOPR','HOPR','DRVH','DRVL'];


# We use the environment variables ARCHAPPL_SHORT_TERM_FOLDER and ARCHAPPL_MEDIUM_TERM_FOLDER to determine the location of the STS and the MTS in the appliance
shorttermstore_plugin_url = 'pb://localhost?name=STS&rootFolder=${ARCHAPPL_SHORT_TERM_FOLDER}&partitionGranularity=PARTITION_HOUR&consolidateOnShutdown=true'
mediumtermstore_plugin_url = 'pb://localhost?name=MTS&rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&partitionGranularity=PARTITION_DAY&hold=2&gather=1'
longtermstore_plugin_url = 'pb://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR'
#longtermstore_plugin_url = 'blackhole://localhost'

def determinePolicy(pvInfoDict):
	pvPolicyDict = {}
	
	pvPolicyDict['samplingPeriod'] = 1.0
	pvPolicyDict['samplingMethod'] = 'MONITOR'
	pvPolicyDict['dataStores'] = [
		shorttermstore_plugin_url, 
		mediumtermstore_plugin_url, 
		longtermstore_plugin_url
		]
	pvPolicyDict['policyName'] = 'Default';
	
	# C is not part of getFieldsArchivedAsPartOfStream
	pvPolicyDict["archiveFields"]=['C', 'HIHI','HIGH','LOW','LOLO','LOPR','HOPR','DRVH','DRVL']
	
	return pvPolicyDict
