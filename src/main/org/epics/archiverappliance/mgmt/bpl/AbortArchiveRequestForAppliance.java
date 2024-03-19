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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Abort any pending requests for archiving this PV in this appliance.
 * @author mshankar
 *
 */
public class AbortArchiveRequestForAppliance implements BPLAction {
	private static final Logger logger = LogManager.getLogger(AbortArchiveRequestForAppliance.class);

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		logger.info("Aborting archiving requests in this appliance for the PV " + pvName);
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			boolean abortedPVWorkflow = configService.getMgmtRuntimeState().abortPVWorkflow(pvName);
			infoValues.put("status", abortedPVWorkflow ? "ok" : "no");
			infoValues.put("desc", "Aborted request for archiving PV " + pvName);
			out.println(JSONValue.toJSONString(infoValues));
		}
	}

}
