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
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

/**
 * Event rate report.
 * 
 * @epics.BPLAction - Return a list of PVs sorted by descending event rate. 
 * @epics.BPLActionParam limit - Limit this report to this many PVs per appliance in the cluster. Optional, if unspecified, there are no limits enforced.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class EventRateReport implements BPLAction {
	private static final Logger logger = Logger.getLogger(EventRateReport.class);
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,
			ConfigService configService) throws IOException {
		String limit = req.getParameter("limit");
		logger.info("Event rate report for " + (limit == null ? "default limit " : ("limit " + limit)));
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		LinkedList<String> eventRateURLs = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			eventRateURLs.add(info.getEngineURL() + "/getEventRateReport" + (limit == null ? "" : ("?limit=" + limit)));
		}		
		try (PrintWriter out = resp.getWriter()) {
			JSONArray neverConnPVs = GetUrlContent.combineJSONArrays(eventRateURLs);
			out.println(JSONValue.toJSONString(neverConnPVs));
		}
	}

}
