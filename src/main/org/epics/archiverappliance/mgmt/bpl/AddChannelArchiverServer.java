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
import org.epics.archiverappliance.mgmt.bpl.cahdlers.ArchivesHandler;
import org.epics.archiverappliance.mgmt.bpl.cahdlers.InfoHandler;
import org.epics.archiverappliance.retrieval.channelarchiver.XMLRPCClient;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Add a channel archiver server.
 * @author mshankar
 *
 */
public class AddChannelArchiverServer implements BPLAction {
	private static Logger logger = Logger.getLogger(AddChannelArchiverServer.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp,ConfigService configService) throws IOException {
		String serverUrl = req.getParameter("channelarchiverserverurl");
		if(serverUrl == null || serverUrl.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}
		logger.info("Adding Channel Archiver server " + serverUrl);
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		
		// Check to see if we already have this server...
		for(String existingServerURL : configService.getChannelArchiverDataServers().keySet()) {
			if(existingServerURL.equals(serverUrl)) {
				logger.info("We already have the Channel Archiver server " + serverUrl);
				resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
				infoValues.put("validation", "We already have the server " + serverUrl + " in the system.");
				try(PrintWriter out = resp.getWriter()) {
					out.println(JSONValue.toJSONString(infoValues));
				}
				return;
			}
		}

		InfoHandler handler = new InfoHandler();
		try {
			XMLRPCClient.archiverInfo(serverUrl, handler);

			ArchivesHandler archivesHandler = new ArchivesHandler();
			XMLRPCClient.archiverArchives(serverUrl, archivesHandler);

			infoValues.put("archives", archivesHandler.getArchives());

			
		} catch(Exception ex) {
			logger.error("Exception adding Channel Archiver server " + serverUrl, ex);
			resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			return;
		}
		
		
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		infoValues.put("Connected", "ok");
		infoValues.put("desc", handler.getDesc());
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(infoValues));
		}
	}
	
}
