/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.URLEncoder;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ChannelArchiverDataServerPVInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

/**
 * Detailed statistics for a PV.
 * 
 * @epics.BPLAction - Get a lot of detailed statistics for a PV. The returned JSON is very UI friendly; but should be usable in a scripting environment.  
 * @epics.BPLActionParam pv - The name of the pv.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 * 
 */
public class PVDetails implements BPLAction {
	private static final Logger logger = Logger.getLogger(PVDetails.class);

	// JSON Array etc are not generic savvy so we get generics errors when we do
	// fancy stuff like so.
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvNameFromRequest = req.getParameter("pv");
		String pvName = PVNames.stripFieldNameFromPVName(pvNameFromRequest);

		ApplianceInfo info = null;
		PVTypeInfo typeInfoForNameFromRequest = configService.getTypeInfoForPV(pvNameFromRequest);
		if(typeInfoForNameFromRequest != null) {
			logger.debug("Was able to find a PVTypeInfo for the name as specified in the request " + pvNameFromRequest);
			pvName = pvNameFromRequest;
			info = configService.getApplianceForPV(pvName);
		} else { 
			String realName = configService.getRealNameForAlias(pvName);
			if(realName != null) pvName = realName;
			logger.debug("Found an alias; using that instead " + pvName);
			info = configService.getApplianceForPV(pvName);
		}
		
		logger.info("Getting the detailed status for PV " + pvName);
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		if(info == null) { 
			pvName = pvNameFromRequest;
			info = configService.getApplianceForPV(pvName);
			if(info == null) { 
				resp.sendError(HttpServletResponse.SC_NOT_FOUND, "Cannot find the appliance archiving " + pvNameFromRequest);
				return;
			}
		}

		String pvDetailsURLSnippet = "/getPVDetails?pv=" + URLEncoder.encode(pvName, "UTF-8");
		try (PrintWriter out = resp.getWriter()) {
			LinkedList<Map<String, String>> result = new LinkedList<Map<String, String>>();
			addDetailedStatus(result, "PV Name", pvNameFromRequest);
			if(!pvName.equals(pvNameFromRequest)) { 
				addDetailedStatus(result, "Alias for ", pvName);
			}
			addDetailedStatus(result, "Instance archiving PV", info.getIdentity());
			PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
			if(typeInfo != null) {
				addDetailedStatus(result, "Archival params creation time:", TimeUtils.convertToHumanReadableString(typeInfo.getCreationTime()));
				addDetailedStatus(result, "Archival params modification time:", TimeUtils.convertToHumanReadableString(typeInfo.getModificationTime()));
				addDetailedStatus(result, "Archiver DBR type (from typeinfo):", typeInfo.getDBRType().toString());
				addDetailedStatus(result, "Is this a scalar:", typeInfo.isScalar() ? "Yes" : "No");
				addDetailedStatus(result, "Number of elements:", Integer.toString(typeInfo.getElementCount()));
				addDetailedStatus(result, "Precision:", Double.toString(typeInfo.getPrecision()));
				addDetailedStatus(result, "Units:", typeInfo.getUnits());
				addDetailedStatus(result, "Is this PV paused:", typeInfo.isPaused() ? "Yes" : "No");
				addDetailedStatus(result, "Sampling method:", typeInfo.getSamplingMethod().toString());
				addDetailedStatus(result, "Sampling period:", Float.toString(typeInfo.getSamplingPeriod()));
				for(String extraFieldName : configService.getExtraFields()) {
					String extraValue = typeInfo.getExtraFields().get(extraFieldName);
					if(extraValue != null) {
						if(extraFieldName.equals("SCAN")){
							try { 
								addDetailedStatus(result, "Extra info - " + extraFieldName + ":", changeSCANValueFromEnumToString(extraValue));
							} catch(Exception ex) { 
								addDetailedStatus(result, "Extra info - " + extraFieldName + ":", extraValue);
							}
						}else{
							addDetailedStatus(result, "Extra info - " + extraFieldName + ":", extraValue);
						}
					}
				}
				String[] archiveFields = typeInfo.getArchiveFields();
				if(archiveFields != null && archiveFields.length > 0) {
					String archiveFieldsStr = typeInfo.obtainArchiveFieldsAsString();
					addDetailedStatus(result, "Archive Fields", archiveFieldsStr);
				}
				List<ChannelArchiverDataServerPVInfo> serverInfos = configService.getChannelArchiverDataServers(pvName);
				if(serverInfos != null && !serverInfos.isEmpty()) { 
					for(ChannelArchiverDataServerPVInfo serverInfo : serverInfos) { 
						addDetailedStatus(result, "External server:", serverInfo.getServerInfo().getServerURL() + "/" + serverInfo.getServerInfo().getIndex());
					}
				}
			} else{
				logger.warn("No PVTypeInfo for pv " + pvName);
			}
			
			JSONArray engineStatusVars = GetUrlContent.getURLContentAsJSONArray(info.getEngineURL() + pvDetailsURLSnippet );
			if(engineStatusVars == null) {
				logger.warn("No status vars from engine using URL " + info.getEngineURL() + pvDetailsURLSnippet);
			} else {
				GetUrlContent.combineJSONArrays(result, engineStatusVars);
			}
			
			if(typeInfo.isPaused()) { 
				logger.debug("Skipping getting pv details from ETL for paused PV " + pvName);
			} else { 
				JSONArray etlStatusVars = GetUrlContent.getURLContentAsJSONArray(info.getEtlURL() + pvDetailsURLSnippet );
				if(etlStatusVars == null) {
					logger.warn("No status vars from ETL using URL " + info.getEtlURL() + pvDetailsURLSnippet);
				} else {
					GetUrlContent.combineJSONArrays(result, etlStatusVars);
				}
			}

			out.println(JSONValue.toJSONString(result));

		}
	}

	private static void addDetailedStatus(
			LinkedList<Map<String, String>> statuses, String name, String value) {
		Map<String, String> obj = new LinkedHashMap<String, String>();
		obj.put("name", name);
		obj.put("value", value);
		obj.put("source", "mgmt");
		statuses.add(obj);
	}

	private String changeSCANValueFromEnumToString(String enumValueStr) {

		/*
		 * choice(menuScanPassive,"Passive") choice(menuScanEvent,"Event")
		 * choice(menuScanI_O_Intr,"I/O Intr")
		 * choice(menuScan10_second,"10 second")
		 * choice(menuScan5_second,"5 second")
		 * choice(menuScan2_second,"2 second")
		 * choice(menuScan1_second,"1 second")
		 * choice(menuScan_5_second,".5 second")
		 * choice(menuScan_2_second,".2 second")
		 * choice(menuScan_1_second,".1 second")
		 */

		String changedValue = "";
		String enumValueIntStr = "" + (int) (Double.parseDouble(enumValueStr));
		switch (enumValueIntStr) {
		case "0": {
			changedValue = "Passive";
			break;
		}
		case "1": {
			changedValue = "Event";
			break;
		}
		case "2": {
			changedValue = "I/O Intr";
			break;
		}
		case "3": {
			changedValue = "10 second";
			break;
		}
		case "4": {
			changedValue = "5 second";
			break;
		}
		case "5": {
			changedValue = "2 second";
			break;
		}
		case "6": {
			changedValue = "1 second";
			break;
		}
		case "7": {
			changedValue = ".5 second";
			break;
		}
		case "8": {
			changedValue = ".2 second";
			break;
		}
		case "9": {
			changedValue = ".1 second";
			break;
		}
		default: {
			changedValue = "unknown";
		}

		}
		return changedValue;
	}

}
