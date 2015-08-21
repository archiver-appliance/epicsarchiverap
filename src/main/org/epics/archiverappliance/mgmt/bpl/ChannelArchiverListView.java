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
import java.util.LinkedList;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Get a list of all Channel Archivers that we know about.
 * @author mshankar
 *
 */
public class ChannelArchiverListView implements BPLAction {
	private static Logger logger = Logger.getLogger(ChannelArchiverListView.class.getName());
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Getting a list of Channel Access servers");
		LinkedList<HashMap<String,String>> infoValues = new LinkedList<HashMap<String,String>>();
		Map<String, String> serverlist = configService.getExternalArchiverDataServers();
		if(serverlist != null) {
			for(String serverURL : serverlist.keySet()) {
				HashMap<String, String> serverInfo = new HashMap<String, String>();
				infoValues.add(serverInfo);
				serverInfo.put("CAUrl", serverURL);
				serverInfo.put("indexes", serverlist.get(serverURL));
			}
		}
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try(PrintWriter out = resp.getWriter()) {
			out.println(JSONValue.toJSONString(infoValues));
		}
		
	}

}
