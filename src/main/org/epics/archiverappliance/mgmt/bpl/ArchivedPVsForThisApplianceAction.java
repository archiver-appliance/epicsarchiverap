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
 * Given a list of PVs, determine those that are being archived.
 * Of course, you can use the status call but that makes calls to the engine etc and can be stressful if you are checking several thousand PVs
 * All this does is check the configservice...
 * 
 * @author mshankar
 *
 */
public class ArchivedPVsForThisApplianceAction implements BPLAction {
	private static final Logger logger = LogManager.getLogger(ArchivedPVsForThisApplianceAction.class);

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Determining PVs that are archived in this appliance");
		LinkedList<String> pvNames = PVsMatchingParameter.getPVNamesFromPostBody(req, configService);
		LinkedList<String> archivedPVs = new LinkedList<String>();
		for(String pvName : pvNames) {
			if(configService.isBeingArchivedOnThisAppliance(pvName)) { 
				archivedPVs.add(pvName);
			}
			
		}
		
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			JSONValue.writeJSONString(archivedPVs, out);
		}
	}
}
