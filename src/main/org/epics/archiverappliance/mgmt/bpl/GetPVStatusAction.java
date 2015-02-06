/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Get the status of a PV.
 * 
 * @epics.BPLAction - Get the status of a PV.
 * @epics.BPLActionParam pv - The name(s) of the pv for which status is to be determined. If a pv is not being archived, you should get back a simple JSON object with a status string of "Not being archived." You can also pass in GLOB wildcards here and multiple PVs as a comma separated list. If you have more PVs that can fit in a GET, send the pv's as a CSV <code>pv=pv1,pv2,pv3</code> as the body of a POST.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class GetPVStatusAction implements BPLAction {
	private static final Logger logger = Logger.getLogger(GetPVStatusAction.class);

	@SuppressWarnings("unchecked")
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		logger.info("Getting the status of pv(s) " + req.getParameter("pv"));
		LinkedList<String> pvNames = PVsMatchingParameter.getMatchingPVs(req, configService, true);
		
		
		HashMap<String, String> pvStatuses = new HashMap<String, String>();
		HashMap<String, LinkedList<String>> pvNamesToAskEngineForStatus = new HashMap<String, LinkedList<String>>();
		HashMap<String, PVTypeInfo> typeInfosForEngineRequests = new HashMap<String, PVTypeInfo>();

		for(String pvName : pvNames) {
			pvName = PVNames.normalizePVName(pvName);
			String pvNameFromRequest = pvName;
			String realName = configService.getRealNameForAlias(pvName);
			if(realName != null) pvName = realName;
			logger.debug("Checking for status for " + pvName);

			ApplianceInfo info = configService.getApplianceForPV(pvName);
			PVTypeInfo typeInfoForPV = null;
			if(info == null) {
				String fieldName = PVNames.getFieldName(pvName);
				if(fieldName != null && !fieldName.equals("")) {
					String pvNameOnly = PVNames.stripFieldNameFromPVName(pvName);
					logger.debug("Looking for appliance for " + pvNameOnly + " to determine field " + fieldName);
					info = configService.getApplianceForPV(pvNameOnly);
					if(info != null) {
						typeInfoForPV = configService.getTypeInfoForPV(pvNameOnly);
						if(typeInfoForPV != null) {
							if(typeInfoForPV.checkIfFieldAlreadySepcified(fieldName)) {
								logger.debug("Standard field, returning status of pv instead " + pvName);
								pvName = pvNameOnly;
							} else { 
								logger.debug("Field " + fieldName + " is not a standard field");
								info = null;
							}
						}
					}
				}
			} else { 
				typeInfoForPV = configService.getTypeInfoForPV(pvName);
			}

			if(info == null) {
				if(configService.doesPVHaveArchiveRequestInWorkflow(pvName)) {
					pvStatuses.put(pvNameFromRequest, "{ \"pvName\": \"" + pvNameFromRequest + "\", \"status\": \"Initial sampling\" }");
				} else {
					pvStatuses.put(pvNameFromRequest, "{ \"pvName\": \"" + pvNameFromRequest + "\", \"status\": \"Not being archived\" }");
				}
			} else {
				if(!pvNamesToAskEngineForStatus.containsKey(info.getEngineURL())) { 
					pvNamesToAskEngineForStatus.put(info.getEngineURL(), new LinkedList<String>());
				}
				pvNamesToAskEngineForStatus.get(info.getEngineURL()).add(pvName);
				typeInfosForEngineRequests.put(pvName, typeInfoForPV);
			}
		}
		
		for(String engineURL : pvNamesToAskEngineForStatus.keySet()) { 
			LinkedList<String> pvNamesToAskEngine = pvNamesToAskEngineForStatus.get(engineURL);
			JSONArray engineStatuses = GetUrlContent.postStringListAndGetContentAsJSONArray(engineURL + "/status", "pv", pvNamesToAskEngine);
			HashMap<String, JSONObject> computedEngineStatueses = new HashMap<String, JSONObject>();
			for(Object engineStatusObj : engineStatuses) { 
				JSONObject engineStatus = (JSONObject) engineStatusObj;
				computedEngineStatueses.put((String) engineStatus.get("pvName"), engineStatus);
			}
			for(String pvNameToAskEngine : pvNamesToAskEngine) {
				PVTypeInfo typeInfo = typeInfosForEngineRequests.get(pvNameToAskEngine);
				assert(typeInfo != null);
				JSONObject pvStatus = computedEngineStatueses.get(pvNameToAskEngine);
				if(pvStatus != null && !pvStatus.isEmpty()) {
					pvStatus.put("appliance", typeInfo.getApplianceIdentity());
					pvStatus.put("pvName", pvNameToAskEngine);
					pvStatus.put("pvNameOnly", pvNameToAskEngine);
					pvStatuses.put(pvNameToAskEngine, pvStatus.toJSONString());
				} else { 
					if(typeInfo != null && typeInfo.isPaused()) { 
						HashMap<String, String> tempStatus = new HashMap<String, String>();
						tempStatus.put("appliance", typeInfo.getApplianceIdentity());
						tempStatus.put("pvName", pvNameToAskEngine);
						tempStatus.put("pvNameOnly", pvNameToAskEngine);
						tempStatus.put("status", "Paused");
						pvStatuses.put(pvNameToAskEngine, JSONValue.toJSONString(tempStatus));
					} 
					else {
						// Here we have a PVTypeInfo but no status from the engine. It could be that we are in that transient period between persisting the PVTypeInfo and opening the CA channel. 
						pvStatuses.put(pvNameToAskEngine, "{ \"pvName\": \"" + pvNameToAskEngine + "\", \"status\": \"Appliance assigned\" }");
					}
				}
			}
		}
		
	
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			out.println("[");
			boolean isFirst = true;
			for(String pvName : pvNames) {  
				if(pvStatuses.containsKey(pvName)) { 
					if(isFirst) { isFirst = false; } else { out.println(","); }
					out.print(pvStatuses.get(pvName));
				}
			}
			out.println("]");
		}
	}
}
