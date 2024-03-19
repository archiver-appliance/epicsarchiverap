#!/usr/bin/python

# policies.py
#
# Author:  M. Shankar, Jan 31, 2012
# Modification History
#        Jan 31, 2012, Shankar: Initial version of policies.py with comments.
#        May 14, 2012, Li, Shankar: Added support for archiving extra fields into policy file.
#
# This is the policies.py used to enforce policies for archiving PVs
# At a very high level, when users request PVs to be archived, the mgmt web app samples the PV to determine event rate and other parameters.
# In addition, various fields of the PV like .NAME, .ADEL, .MDEL, .RTYP etc are also obtained
# These are passed to this python script as a dictionary argument to a method called determinePolicy
# The variable name in the python environment for this information is 'pvInfo' (so use other variable names etc.).
# The method is expected to use this information to make decisions on various archiving parameters. 
# The result is expected to be another dictionary that is placed into the variable called "pvPolicy".
# Optionally, fields in addition to the VAL field that are to be archived with the PV are passed in as a property of pvPolicy called 'archiveFields'
# If the user overrides the policy, this is communicated in the pvinfo as a property called 'policyName'
#
# In addition, this script must communicate the list of available policies to the JVM as another method called getPolicyList which takes no arguments.
# The results of this method is placed into a variable called called 'pvPolicies'.  
# The dictionary is a name to description mapping - the description is used in the UI; the name is what is communicated to determinePolicy as a user override
#
# In addition, this script must communicate the list of fields that are to be archived as part of the stream in a method called getFieldsArchivedAsPartOfStream.
# The results of this method is placed into a list variable called called 'pvStandardFields'.  


import sys
import os

# Generate a list of policy names. This is used to feed the dropdown in the UI.
def getPolicyList():
	pvPoliciesDict = {}
	pvPoliciesDict['Default'] = 'The default policy'
	pvPoliciesDict['BPMS'] = 'BPMS that generate more than 1GB a year'
	pvPoliciesDict['2HzPVs'] = 'PVs with an event rate more than 2Hz'
	pvPoliciesDict['3DaysMTSOnly'] = 'Store data for 3 days upto the MTS only.'
	return pvPoliciesDict

# Define a list of fields that will be archived as part of every PV.
# The data for these fields will included in the stream for the PV.
# We also make an assumption that the data type for these fields is the same as that of the .VAL field
def getFieldsArchivedAsPartOfStream():
	 return ['HIHI','HIGH','LOW','LOLO','LOPR','HOPR','DRVH','DRVL'];


# We use the environment variables ARCHAPPL_SHORT_TERM_FOLDER and ARCHAPPL_MEDIUM_TERM_FOLDER to determine the location of the STS and the MTS in the appliance
shorttermstore_plugin_url = 'pb://localhost?name=STS&rootFolder=${ARCHAPPL_SHORT_TERM_FOLDER}&partitionGranularity=PARTITION_HOUR&consolidateOnShutdown=true'
mediumtermstore_plugin_url = 'pb://localhost?name=MTS&rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&partitionGranularity=PARTITION_DAY&hold=2&gather=1'
longtermstore_plugin_url = 'pb://localhost?name=LTS&rootFolder=${ARCHAPPL_LONG_TERM_FOLDER}&partitionGranularity=PARTITION_YEAR'
#longtermstore_plugin_url = 'blackhole://localhost'

def determinePolicy(pvInfoDict):
	pvPolicyDict = {}
	
	userPolicyOverride = ''
	if 'policyName' in pvInfoDict:
		userPolicyOverride = pvInfoDict['policyName']

	if userPolicyOverride == '2HzPVs' or (userPolicyOverride == '' and pvInfoDict['eventRate'] > 2.0):
		pvPolicyDict['samplingPeriod'] = 1.0
		pvPolicyDict['samplingMethod'] = 'MONITOR'
		pvPolicyDict['dataStores'] = [
			shorttermstore_plugin_url, 
			mediumtermstore_plugin_url, 
			longtermstore_plugin_url
			]
		pvPolicyDict['policyName'] = '2HzPVs';
	elif userPolicyOverride == 'Default' or (userPolicyOverride == '' and pvInfoDict['pvName'] == 'mshankar:arch:sine'):
		pvPolicyDict['samplingPeriod'] = 1.0
		pvPolicyDict['samplingMethod'] = 'MONITOR'
		pvPolicyDict['dataStores'] = [
			shorttermstore_plugin_url, 
			mediumtermstore_plugin_url, 
			longtermstore_plugin_url + "&pp=firstSample&pp=firstSample_3600"
			]
		pvPolicyDict['policyName'] = 'Default';
	elif userPolicyOverride == 'BPMS' or (userPolicyOverride == '' and pvInfoDict['pvName'].startswith('BPMS') and pvInfoDict['storageRate'] > 35):
		# We limit BPM PVs to 1GB/year (34 bytes/sec)
		# We reduce the sampling rate (and hence increase the sampling period) to cater to this. 
		pvPolicyDict['samplingPeriod'] = pvInfoDict['storageRate']/(35*pvInfoDict['eventRate'])
		pvPolicyDict['samplingMethod'] = 'MONITOR'
		pvPolicyDict['dataStores'] = [
			shorttermstore_plugin_url, 
			mediumtermstore_plugin_url, 
			longtermstore_plugin_url
			]
		pvPolicyDict['policyName'] = 'BPMS';
	elif userPolicyOverride == '3DaysMTSOnly':
		pvPolicyDict['samplingPeriod'] = 1.0
		pvPolicyDict['samplingMethod'] = 'MONITOR'
		pvPolicyDict['dataStores'] = [
			shorttermstore_plugin_url, 
			# We want to store 3 days worth of data in the MTS.
			'pb://localhost?name=MTS&rootFolder=${ARCHAPPL_MEDIUM_TERM_FOLDER}&partitionGranularity=PARTITION_DAY&hold=4&gather=1', 
			'blackhole://localhost?name=LTS'
			]
		pvPolicyDict['policyName'] = '2HzPVs';
	else:
		pvPolicyDict['samplingPeriod'] = 1.0
		pvPolicyDict['samplingMethod'] = 'MONITOR'
		pvPolicyDict['dataStores'] = [
			shorttermstore_plugin_url, 
			mediumtermstore_plugin_url, 
			longtermstore_plugin_url
			]
		pvPolicyDict['policyName'] = 'Default';
	 
	archiveFields=[]
	
	if "RTYP" not in pvInfoDict:
		pvPolicyDict["archiveFields"]=archiveFields
	else:
		pvRTYP=pvInfoDict["RTYP"]
		if pvRTYP=="ai":
			archiveFields=['HIHI','HIGH','LOW','LOLO','LOPR','HOPR']
		elif pvRTYP=="ao":
			archiveFields=['HIHI','HIGH','LOW','LOLO','LOPR','HOPR','DRVH','DRVL']
		elif pvRTYP=="calc":
			archiveFields=['HIHI','HIGH','LOW','LOLO','LOPR','HOPR']
		elif pvRTYP=="calcout":
			archiveFields=['HIHI','HIGH','LOW','LOLO','LOPR','HOPR']
		elif pvRTYP=="longin":
			archiveFields=['HIHI','HIGH','LOW','LOLO','LOPR','HOPR']
		elif pvRTYP=="longout":
			archiveFields=['HIHI','HIGH','LOW','LOLO','LOPR','HOPR','DRVH','DRVL']
		elif pvRTYP=="dfanout":
			archiveFields=['HIHI','HIGH','LOW','LOLO','LOPR','HOPR']
		elif pvRTYP=="sub":
			archiveFields=['HIHI','HIGH','LOW','LOLO','LOPR','HOPR']
		pvPolicyDict["archiveFields"]=archiveFields

	return pvPolicyDict
