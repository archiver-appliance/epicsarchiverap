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
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

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
	private static final Logger logger = LogManager.getLogger(GetPVStatusAction.class);

	@SuppressWarnings("unchecked")
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		logger.info("Getting the status of pv(s) " + req.getParameter("pv"));
		LinkedList<String> pvNames = PVsMatchingParameter.getMatchingPVs(req, configService, true, -1);
		
		
		HashMap<String, Map<String, String>> pvStatuses = new LinkedHashMap<>();
		HashMap<String, LinkedList<String>> pvNamesToAskEngineForStatus = new LinkedHashMap<String, LinkedList<String>>();
		HashMap<String, PVTypeInfo> typeInfosForEngineRequests = new LinkedHashMap<String, PVTypeInfo>();

		HashMap<String, LinkedList<String>> realName2NameFromRequest = new LinkedHashMap<String, LinkedList<String>>();

		getPVStatuses(configService, pvNames, pvStatuses, pvNamesToAskEngineForStatus, typeInfosForEngineRequests, realName2NameFromRequest);

		JSONArray json = new JSONArray();
		json.addAll(pvStatuses.values());

		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		try (PrintWriter out = resp.getWriter()) {
			out.print(json.toJSONString());
		}
	}

	public static void getPVStatuses(ConfigService configService, LinkedList<String> pvNames,
	                                 HashMap<String, Map<String, String>> pvStatuses, HashMap<String, LinkedList<String>> pvNamesToAskEngineForStatus,
	                                 HashMap<String, PVTypeInfo> typeInfosForEngineRequests, HashMap<String, LinkedList<String>> realName2NameFromRequest) {
		for(String pvName : pvNames) {
			String pvNameFromRequest = pvName;

			// Get rid of .VAL and the V4 prefix
			pvName = PVNames.normalizePVName(pvName);
			pvName = PVNames.stripPrefixFromName(pvName);
			addInverseNameMapping(pvNameFromRequest, pvName, realName2NameFromRequest);

			PVTypeInfo typeInfoForPV = PVNames.determineAppropriatePVTypeInfo(pvName, configService);
			if(typeInfoForPV != null) {
				pvName = typeInfoForPV.getPvName();
				addInverseNameMapping(pvNameFromRequest, pvName, realName2NameFromRequest);
				ApplianceInfo info = configService.getAppliance(typeInfoForPV.getApplianceIdentity());

				if(!pvNamesToAskEngineForStatus.containsKey(info.getEngineURL())) {
					pvNamesToAskEngineForStatus.put(info.getEngineURL(), new LinkedList<String>());
				}
				pvNamesToAskEngineForStatus.get(info.getEngineURL()).add(pvName);
				typeInfosForEngineRequests.put(pvName, typeInfoForPV);
			} else {
				Map<String, String> pvStatusInfo = new LinkedHashMap<String, String>();
				pvStatusInfo.put("pvName", pvNameFromRequest);
				if (PVNames.determineIfPVInWorkflow(pvName, configService)) {
					pvStatusInfo.put("status", "Initial sampling");
				} else {
					pvStatusInfo.put("status", "Not being archived");
				}
				pvStatuses.put(pvNameFromRequest, pvStatusInfo);
			}
		}
		for(String engineURL : pvNamesToAskEngineForStatus.keySet()) {
			LinkedList<String> pvNamesToAskEngine = pvNamesToAskEngineForStatus.get(engineURL);
			JSONArray engineStatuses = null;
			boolean instanceDown = false;
			try {
				engineStatuses = GetUrlContent.postStringListAndGetJSON(engineURL + "/status", "pv", pvNamesToAskEngine);
			} catch(IOException ex) {
				instanceDown = true;
				logger.warn("Exception getting status from engine " + engineURL, ex);
			}
			// Convert list of statuses from engine to hashmap
			HashMap<String, JSONObject> computedEngineStatueses = new LinkedHashMap<String, JSONObject>();
			if(engineStatuses != null) {
				for(Object engineStatusObj : engineStatuses) {
					JSONObject engineStatus = (JSONObject) engineStatusObj;
					computedEngineStatueses.put((String) engineStatus.get("pvName"), engineStatus);
				}
			}
			for(String pvNameToAskEngine : pvNamesToAskEngine) {
				PVTypeInfo typeInfo = typeInfosForEngineRequests.get(pvNameToAskEngine);
				JSONObject pvStatus = computedEngineStatueses.get(pvNameToAskEngine);
				LinkedList<String> pvNamesForResult = new LinkedList<String>();
				if(pvNames.contains(pvNameToAskEngine)) {
					// User made a status request using the same name we sent to the engine.
					pvNamesForResult.add(pvNameToAskEngine);
				}
				if(realName2NameFromRequest.containsKey(pvNameToAskEngine)) {
					// Add all the aliases/field names etc.
					pvNamesForResult.addAll(realName2NameFromRequest.get(pvNameToAskEngine));
				}
				for(String pvNameForResult : pvNamesForResult) {
					Map<String, String> pvStatusInfo = new LinkedHashMap<String, String>();
					if(pvStatus != null && !pvStatus.isEmpty()) {
						logger.debug("Found status from engine for " + pvNameToAskEngine);
						pvStatus.forEach((k, v) -> {
							pvStatusInfo.put(String.valueOf(k), String.valueOf(v));
						});
						pvStatusInfo.put("appliance", typeInfo.getApplianceIdentity());
						pvStatusInfo.put("pvName", pvNameForResult);
						pvStatusInfo.put("pvNameOnly", pvNameToAskEngine);
						pvStatuses.put(pvNameForResult, pvStatusInfo);
					} else {
						logger.debug("Did not find status from engine for " + pvNameToAskEngine);
						if(typeInfo != null && typeInfo.isPaused()) {
							HashMap<String, String> tempStatus = new LinkedHashMap<String, String>();
							tempStatus.put("appliance", typeInfo.getApplianceIdentity());
							tempStatus.put("pvName", pvNameForResult);
							tempStatus.put("pvNameOnly", pvNameToAskEngine);
							tempStatus.put("status", "Paused");
							pvStatuses.put(pvNameForResult, tempStatus);
						} else {
							logger.info("Did not find status from engine for " + pvNameToAskEngine + " adding default status for " + pvNameForResult);
							Map<String, String> missingPvStatusInfo = new LinkedHashMap<String, String>();
							missingPvStatusInfo.put("pvName", pvNameForResult);
							// Here we have a PVTypeInfo but no status from the engine.
							if(instanceDown) {
								missingPvStatusInfo.put("status", "Appliance Down");
								// It could mean that the engine component archiving this PV is down.
								pvStatuses.put(pvNameForResult, missingPvStatusInfo);
							} else {
								missingPvStatusInfo.put("status", "Appliance assigned");
								// It could be that we are in that transient period between persisting the PVTypeInfo and opening the CA channel.
								pvStatuses.put(pvNameForResult, missingPvStatusInfo);
							}
						}
					}
					logger.debug(pvStatuses);
				}
			}
		}
	}


	/**
	 * There is a 1-many mapping between the name the user asks for and the internals.
	 * When sending the data back, we need to take the "real" information and create an entry for each user request.
	 * This method maintains this list.  
	 * @param pvNameFromRequest
	 * @param pvName
	 * @param realName2NameFromRequest
	 */
	private static void addInverseNameMapping(String pvNameFromRequest, String pvName, HashMap<String, LinkedList<String>> realName2NameFromRequest) { 
		if(!pvName.equals(pvNameFromRequest)) { 
			if(!realName2NameFromRequest.containsKey(pvName)) { 
				realName2NameFromRequest.put(pvName, new LinkedList<String>());
			}
			realName2NameFromRequest.get(pvName).add(pvNameFromRequest);
		}
	}
}
