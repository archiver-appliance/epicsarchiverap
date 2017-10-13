/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.reports;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

/**
 * Creation time and paused status of all PVs in this appliance 
 * 
 * @author mshankar
 *
 */
public class CreationTimeReportForAppliance implements BPLAction {
	private static final Logger logger = Logger.getLogger(CreationTimeReportForAppliance.class);

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		logger.info("Getting the creation time for PV's in this appliance");
		resp.setContentType(MimeTypeConstants.APPLICATION_JSON);

		Pattern pattern = null;
		if(req.getParameter("regex") != null) { 
			String nameToMatch = req.getParameter("regex");
			logger.debug("Finding PV's for regex " + nameToMatch);
			pattern = Pattern.compile(nameToMatch);
		}
		
		Set<String> pausedPVs = configService.getPausedPVsInThisAppliance();
		try (PrintWriter out = resp.getWriter()) {
			out.println("[");
			boolean first = true;
			for(String pvName : configService.getPVsForThisAppliance()) { 
				if(pattern != null && !pattern.matcher(pvName).matches()) continue;

				if(first) { first = false; } else { out.println(","); }
				out.println("{");
				out.print("\"pvName\": \"");
				out.print(pvName);
				out.println("\",");
				out.print("\"creationTS\": ");
				out.print(configService.getTypeInfoForPV(pvName).getCreationTime().getTime()/1000);
				out.println(",");
				out.print("\"paused\": ");
				out.print(pausedPVs.contains(pvName) ? "true" : "false" );
				out.println();
				out.print("}");
			}
			out.println("]");
		}
	}
}
