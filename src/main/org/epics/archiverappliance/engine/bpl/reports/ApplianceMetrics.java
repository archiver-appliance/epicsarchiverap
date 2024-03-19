/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.epics.EngineMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

/**
 * Summary appliance metrics for the engine.
 * @author mshankar
 *
 */
public class ApplianceMetrics implements BPLAction {

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
		try (PrintWriter out = resp.getWriter()) {
			out.println(EngineMetrics.computeEngineMetrics(configService.getEngineContext(), configService).toJSONString());
		}
	}
}
