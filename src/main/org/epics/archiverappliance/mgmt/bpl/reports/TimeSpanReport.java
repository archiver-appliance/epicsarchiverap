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
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.PoorMansProfiler;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Archiving time span. 
 * 
 * @epics.BPLAction - Archiving time span; when we first added the PV to the system, last known timestamp (if available), paused or not. 
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class TimeSpanReport implements BPLAction {
	private static final Logger logger = LogManager.getLogger(TimeSpanReport.class);

	@SuppressWarnings("unchecked")
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Getting the time spans for PVs in the cluster");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		
		String nameToMatch = null;
		if(req.getParameter("regex") != null) { 
			nameToMatch = req.getParameter("regex");
			logger.debug("Finding PV's for regex " + nameToMatch);
		}

		LinkedList<String> lastKnownTimestampsURLs = new LinkedList<String>();
		LinkedList<String> creationTimeURLs = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			lastKnownTimestampsURLs.add(info.getEngineURL() + "/getLastKnownTimeStampReport" + (nameToMatch == null ? "" : "?regex="+URLEncoder.encode(nameToMatch, "UTF-8")));
			creationTimeURLs.add(info.getMgmtURL() + "/getCreationReportForAppliance" + (nameToMatch == null ? "" : "?regex="+URLEncoder.encode(nameToMatch, "UTF-8")));
		}		

		PoorMansProfiler pf = new PoorMansProfiler();
		JSONArray lastKnownTS = GetUrlContent.combineJSONArrays(lastKnownTimestampsURLs);
		pf.mark("After last known time stamps from the engine");
		JSONArray creationTimes = GetUrlContent.combineJSONArrays(creationTimeURLs);
		pf.mark("After creation times from mgmt");
		
		Map<String, JSONObject> ret = new TreeMap<String, JSONObject>();
		try (PrintWriter out = resp.getWriter()) {
			for(Object crObj : creationTimes) { 
				JSONObject crHash = (JSONObject) crObj;
				ret.put((String) crHash.get("pvName"), crHash);
			}
			pf.mark("After inserting creation times");

			for(Object lsObj : lastKnownTS) { 
				JSONObject lsHash = (JSONObject) lsObj;
				String pvName = (String) lsHash.get("pvName");
				if(ret.containsKey(pvName)) { 
					ret.get(pvName).putAll(lsHash);
				} else { 
					logger.warn("We have a last known time stamp but no typeinfo for PV " + pvName);
				}
			}
			pf.mark("After inserting last known times");
			
			JSONValue.writeJSONString(ret, out);
			pf.mark("After sending data");
		}
		logger.info(pf.toString());
	}
}
