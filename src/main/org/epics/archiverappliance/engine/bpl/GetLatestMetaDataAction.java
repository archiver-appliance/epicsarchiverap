/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.EngineContext;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

/**
 * Get the latest metadata from the various sources within the engine..
 * @author mshankar
 *
 */
public class GetLatestMetaDataAction implements BPLAction {
	private static final Logger logger = Logger.getLogger(GetLatestMetaDataAction.class);
	
	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		EngineContext engineContext = configService.getEngineContext();
		if(engineContext.getChannelList().containsKey(pvName)){
			ArchiveChannel archiveChannel = engineContext.getChannelList().get(pvName);
			HashMap<String, String> retVal = archiveChannel.getLatestMetadata();
			resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
			try(PrintWriter out = resp.getWriter()) {
				out.println(JSONValue.toJSONString(retVal));
			}
			return;
		}

		logger.debug("No data for PV " + pvName + " in this engine.");
		resp.sendError(HttpServletResponse.SC_NOT_FOUND);
		return;
	}	
}
