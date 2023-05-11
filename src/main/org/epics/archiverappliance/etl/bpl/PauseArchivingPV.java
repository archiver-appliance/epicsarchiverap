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
import java.util.LinkedList;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;
/**
 * Remove the ETL jobs for this PV.
 * @author Luofeng  Li 
 *
 */
public class PauseArchivingPV implements BPLAction {
	private static final Logger logger = LogManager.getLogger(PauseArchivingPV.class);
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvNamesStr = req.getParameter("pv");
		if(pvNamesStr == null || pvNamesStr.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String[] pvNames = pvNamesStr.split(",");

		LinkedList<HashMap<String, Object>> statuses = new LinkedList<HashMap<String, Object>>();
		for(String pvName : pvNames) { 
			HashMap<String, Object> infoValues = new HashMap<String, Object>();
			infoValues.put("pvName", pvName);
			statuses.add(infoValues);
			try {
				logger.debug("Pausing PV " + pvName + " in ETL");
				configService.getETLLookup().deleteETLJobs(pvName);
				infoValues.put("status", "ok");
				infoValues.put("desc", "Successfully removed PV " + pvName + " from the cluster");
			} catch(Exception ex) {
				logger.error("Exception pausing PV in etl " + pvName, ex);
				infoValues.put("status", "failed");
				infoValues.put("validation", ex.getMessage());
			}
		}
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(statuses));
		}
	}
}
