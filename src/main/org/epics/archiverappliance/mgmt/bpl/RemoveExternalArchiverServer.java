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

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Remove the entry for a external Archiver Data Server from the config database
 * Note that the entire cluster may need to be restarted for this change to take effect.
 * @author mshankar
 *
 */
public class RemoveExternalArchiverServer implements BPLAction {
	private static Logger logger = Logger.getLogger(RemoveExternalArchiverServer.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String serverUrl = req.getParameter("channelarchiverserverurl");
		if(serverUrl == null || serverUrl.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		
		String archivesCSV = req.getParameter("archives");
		if(archivesCSV == null || archivesCSV.equals("")) {
			logger.error("No archives parameter specified");
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		logger.info("Removing channel archiver server archives " + archivesCSV + " for server " + serverUrl);
		
		configService.removeExternalArchiverDataServer(serverUrl, archivesCSV);
		

		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		infoValues.put("status", "ok");
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(infoValues));
		}
	}
}
