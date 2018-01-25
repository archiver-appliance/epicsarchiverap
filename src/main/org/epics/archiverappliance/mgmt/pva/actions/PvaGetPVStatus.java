/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.pva.actions;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVNames;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.mgmt.bpl.PVsMatchingParameter;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.epics.nt.NTTable;
import org.epics.nt.NTURI;
import org.epics.pvaccess.server.rpc.RPCResponseCallback;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
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
public class PvaGetPVStatus implements PvaAction {
	private static final Logger logger = Logger.getLogger(PvaGetPVStatus.class);
	
	public static final String NAME = "PVStatus";
	
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



	@Override
	public String getName() {
		return NAME;
	}



	@SuppressWarnings("unchecked")
	@Override
	public void request(PVStructure args, RPCResponseCallback callback, ConfigService configService) {
		Map<String, String> requestParameters = new HashMap<>();
		if (NTTable.isCompatible(args)) {
			requestParameters.put("pv", String.join(",",
					NTUtil.extractStringList(NTTable.wrap(args).getColumn(PVStringArray.class, "pv"))));
		} else if (NTURI.isCompatible(args)) {

		} else {
			// TODO return
		}
		LinkedList<String> pvNames = PVsMatchingParameter.getMatchingPVs(requestParameters, configService, true, -1);
		

		HashMap<String, Map<String, String>> pvStatuses = new HashMap<String, Map<String, String>>();
		HashMap<String, LinkedList<String>> pvNamesToAskEngineForStatus = new HashMap<String, LinkedList<String>>();
		HashMap<String, PVTypeInfo> typeInfosForEngineRequests = new HashMap<String, PVTypeInfo>();
		HashMap<String, LinkedList<String>> realName2NameFromRequest = new HashMap<String, LinkedList<String>>();
		
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
					pvStatuses.put(pvNameFromRequest, pvStatusInfo);
				} else {
					pvStatusInfo.put("status", "Initial sampling");
					pvStatuses.put(pvNameFromRequest, pvStatusInfo);
				}
			}
		}
		for(String engineURL : pvNamesToAskEngineForStatus.keySet()) {
			LinkedList<String> pvNamesToAskEngine = pvNamesToAskEngineForStatus.get(engineURL);
			JSONArray engineStatuses = null;
			boolean instanceDown = false;
			try { 
				engineStatuses = GetUrlContent.postStringListAndGetContentAsJSONArray(engineURL + "/status", "pv", pvNamesToAskEngine);
			} catch(IOException ex) { 
				instanceDown = true;
				logger.warn("Exception getting status from engine " + engineURL, ex);
			}
			// Convert list of statuses from engine to hashmap
			HashMap<String, JSONObject> computedEngineStatueses = new HashMap<String, JSONObject>();
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
					Map<String, String> pvStatusInfo = new HashMap<String, String>();
					if(pvStatus != null && !pvStatus.isEmpty()) {
						logger.info("Found status from engine for " + pvNameToAskEngine);
						pvStatus.forEach((k, v) -> {
							pvStatusInfo.put(String.valueOf(k), String.valueOf(v));
						});
						pvStatusInfo.put("appliance", typeInfo.getApplianceIdentity());
						pvStatusInfo.put("pvName", pvNameForResult);
						pvStatusInfo.put("pvNameOnly", pvNameToAskEngine);
						pvStatuses.put(pvNameForResult, pvStatusInfo);
					} else { 
						logger.info("Did not find status from engine for " + pvNameToAskEngine);
						if(typeInfo != null && typeInfo.isPaused()) { 
							HashMap<String, String> tempStatus = new HashMap<String, String>();
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
					/**
					 *  key: test_664 
					 *  value: {"lastRotateLogs":"Never",
					 *  		"appliance":"appliance0",
					 *  		"pvName":"test_664",
					 *  		"pvNameOnly":"test_664",
					 *  		"connectionState":"true",
					 *  		"lastEvent":"Jan\/24\/2018 15:52:26 -05:00",
					 *  		"samplingPeriod":"1.0",
					 *  		"isMonitored":"true",
					 *  		"connectionLastRestablished":"Never",
					 *  		"connectionFirstEstablished":"Jan\/24\/2018 15:45:03 -05:00",
					 *  		"connectionLossRegainCount":"0",
					 *  		"status":"Being archived"}
					 */
					pvStatuses.forEach((k,v) -> {System.out.println(" key: " + k + " value: " + v);});
				}
			}
		}
		NTTable resultTable = NTTable.createBuilder()
				.addColumn("pv", ScalarType.pvString)
				.addColumn("status", ScalarType.pvString)
				.addColumn("appliance", ScalarType.pvString)
				.addColumn("connectionState", ScalarType.pvString)
				.addColumn("lastEvent", ScalarType.pvString)
				.addColumn("samplingPeriod", ScalarType.pvString)
				.addColumn("isMonitored", ScalarType.pvString)
				.addColumn("connectionFirstEstablished", ScalarType.pvString)
				.addColumn("connectionLossRegainCount", ScalarType.pvString)
				.addColumn("connectionLastRestablished", ScalarType.pvString)
				.create();
		LinkedList<String> pv = new LinkedList<>();
		LinkedList<String> status = new LinkedList<>();
		LinkedList<String> appliance = new LinkedList<>();
		LinkedList<String> connectionState = new LinkedList<>();
		LinkedList<String> lastEvent = new LinkedList<>();
		LinkedList<String> samplingPeriod = new LinkedList<>();
		LinkedList<String> isMonitored = new LinkedList<>();
		LinkedList<String> connectionFirstEstablished = new LinkedList<>();
		LinkedList<String> connectionLossRegainCount = new LinkedList<>();
		LinkedList<String> connectionLastRestablished = new LinkedList<>();
		pvStatuses.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEachOrdered(entry -> {
			pv.add(entry.getKey());
			status.add(entry.getValue().get("status"));
			appliance.add(entry.getValue().get("appliance"));
			connectionState.add(entry.getValue().get("connectionState"));
			lastEvent.add(entry.getValue().get("lastEvent"));
			samplingPeriod.add(entry.getValue().get("samplingPeriod"));
			isMonitored.add(entry.getValue().get("isMonitored"));
			connectionFirstEstablished.add(entry.getValue().get("connectionFirstEstablished"));
			connectionLossRegainCount.add(entry.getValue().get("connectionLossRegainCount"));
			connectionLastRestablished.add(entry.getValue().get("connectionLastRestablished"));
		});
		resultTable.getColumn(PVStringArray.class, "pv").put(0, pv.size(), pv.toArray(new String[pv.size()]), 0);
		resultTable.getColumn(PVStringArray.class, "status").put(0, status.size(),
				status.toArray(new String[status.size()]), 0);
		resultTable.getColumn(PVStringArray.class, "appliance").put(0, appliance.size(),
				appliance.toArray(new String[appliance.size()]), 0);
		resultTable.getColumn(PVStringArray.class, "connectionState").put(0, connectionState.size(),
				connectionState.toArray(new String[connectionState.size()]), 0);
		resultTable.getColumn(PVStringArray.class, "lastEvent").put(0, lastEvent.size(),
				lastEvent.toArray(new String[lastEvent.size()]), 0);
		resultTable.getColumn(PVStringArray.class, "samplingPeriod").put(0, samplingPeriod.size(),
				samplingPeriod.toArray(new String[samplingPeriod.size()]), 0);
		resultTable.getColumn(PVStringArray.class, "isMonitored").put(0, isMonitored.size(),
				isMonitored.toArray(new String[isMonitored.size()]), 0);
		resultTable.getColumn(PVStringArray.class, "connectionFirstEstablished").put(0,
				connectionFirstEstablished.size(),
				connectionFirstEstablished.toArray(new String[connectionFirstEstablished.size()]), 0);
		resultTable.getColumn(PVStringArray.class, "connectionLossRegainCount").put(0, connectionLossRegainCount.size(),
				connectionLossRegainCount.toArray(new String[connectionLossRegainCount.size()]), 0);
		resultTable.getColumn(PVStringArray.class, "connectionLastRestablished").put(0,
				connectionLastRestablished.size(),
				connectionLastRestablished.toArray(new String[connectionLastRestablished.size()]), 0);
		callback.requestDone(StatusFactory.getStatusCreate().getStatusOK(), resultTable.getPVStructure());
	}
}
