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
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

/**
 * Abort any pending requests for archiving this PV.
 * 
 * @epics.BPLAction - Abort any pending requests for archiving this PV.
 * @epics.BPLActionParam pv - The name of the PV.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class AbortArchiveRequest implements BPLAction {
	private static final Logger logger = LogManager.getLogger(AbortArchiveRequest.class);

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		// String pvNameFromRequest = pvName;
		String realName = configService.getRealNameForAlias(pvName);
		if(realName != null) pvName = realName;
		
		logger.info("Aborting archiving requests for the PV " + pvName);
		LinkedList<String> abortPVURLs = new LinkedList<String>();
		for(ApplianceInfo info : configService.getAppliancesInCluster()) {
			abortPVURLs.add(info.getMgmtURL() + "/abortArchivingPVForThisAppliance?pv=" + URLEncoder.encode(pvName, "UTF-8"));
		}		

		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			boolean abortSuccessful = false;
			for(String abortPVURL : abortPVURLs) {
				JSONObject abortResp = GetUrlContent.getURLContentAsJSONObject(abortPVURL);
				if(abortResp != null && abortResp.containsKey("status") && abortResp.get("status").equals("ok")) { 
					abortSuccessful = true;
				}
			}
			infoValues.put("status", abortSuccessful ? "ok" : "no");
			infoValues.put("desc", "Aborted request for archiving PV " + pvName);
			out.println(JSONValue.toJSONString(infoValues));
		}
	}

}
