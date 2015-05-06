/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/


package org.epics.archiverappliance.etl.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;
/**
 * Remove the ETL jobs for this PV.
 * @author Luofeng  Li 
 *
 */
public class PauseArchivingPV implements BPLAction {
	private static final Logger logger = Logger.getLogger(PauseArchivingPV.class);
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
		if(typeInfo == null) {
			logger.debug("Unable to find typeinfo for PV...");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		try (PrintWriter out = resp.getWriter()) {
			// Remove any ETL jobs from the runtime state. 
			configService.getETLLookup().deleteETLJobs(pvName);
			infoValues.put("status", "ok");
			infoValues.put("desc", "Successfully removed PV " + pvName + " from the cluster");
			out.println(JSONValue.toJSONString(infoValues));
			return;
		}
		
	}
}
