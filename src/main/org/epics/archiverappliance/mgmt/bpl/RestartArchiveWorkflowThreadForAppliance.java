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
 * Restart the archive PV workflow for this thread.
 * Ocassionally, the main archive PV workflow thread seems to get stuck and does not process any new PV requests ( might be a bug )
 * Restarting the appliance typically fixes this.  
 * Use this BPL to restart just the archive PV workflow thread for this appliance ( without having to restart the appliance ).
 * Not this does not reinitiate the existing archive PV requests; if you wish to continue these pending requests please simply restart the appliance. 
 * @author mshankar
 *
 */
public class RestartArchiveWorkflowThreadForAppliance implements BPLAction {
	private static final Logger logger = LogManager.getLogger(RestartArchiveWorkflowThreadForAppliance.class);

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Restarting the archive PV workflow thread for this appliance");
		HashMap<String, Object> infoValues = new HashMap<String, Object>();
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			configService.getMgmtRuntimeState().abortAllAndRestartArchiveRequestsThread();
			infoValues.put("status", "ok");
			out.println(JSONValue.toJSONString(infoValues));
		}
	}

}
