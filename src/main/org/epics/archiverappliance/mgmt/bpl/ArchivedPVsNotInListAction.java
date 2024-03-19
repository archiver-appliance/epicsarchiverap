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
 * Given a list of PVs, determine those that are being archived but are not in the incoming list.
 * Useful if you generate a list of PV's to be archived from the IOC configuration somehow and need to make sure that PV's being archived are only those in this list. 
 * 
 * @epics.BPLAction - Given a list of PVs, determine those that are being archived but are not in the incoming list.
 * @epics.BPLActionParam pv - A list of pv names. Send as a CSV using a POST, or as a JSON.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class ArchivedPVsNotInListAction implements BPLAction {
	private static final Logger logger = LogManager.getLogger(ArchivedPVsNotInListAction.class);

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Determining PVs that are archived but are not in list.");
		LinkedList<String> incomingPVNamesList = PVsMatchingParameter.getPVNamesFromPostBody(req, configService);
		logger.debug("Incoming list has " + incomingPVNamesList.size() + "PV names");
		if(incomingPVNamesList.size() <= 0){ 
			logger.error("Incoming list cannnot be empty for the action " + this.getClass().getName());
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;			
		}
		LinkedList<String> archivedPVsNotInList = new LinkedList<String>();
		for(String pvName : configService.getAllPVs()) {
			if(!incomingPVNamesList.contains(pvName)) { 
				archivedPVsNotInList.add(pvName);
			}
		}
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			JSONValue.writeJSONString(archivedPVsNotInList, out);
		}
	}
}
