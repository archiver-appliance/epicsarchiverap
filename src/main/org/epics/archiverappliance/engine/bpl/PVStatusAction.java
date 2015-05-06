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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.epics.EngineChannelStatus;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.engine.pv.EPICSV4.ArchiveEngine_EPICSV4;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

/**
 * BPL for getting the status of a PV.
 * @author mshankar
 *
 */
public class PVStatusAction implements BPLAction {
	private static Logger logger = Logger.getLogger(PVStatusAction.class.getName());

	@Override
	public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService) throws IOException {
		String pvName = req.getParameter("pv");
		if(pvName == null || pvName.equals("")) {
			resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
			return;
		}

		try {
			PVTypeInfo typeInfoForPV = configService.getTypeInfoForPV(pvName);
			if(typeInfoForPV == null) {
				logger.error("Could not find pv type info for PV " + pvName);
				resp.sendError(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
			ArchDBRTypes dbrType = typeInfoForPV.getDBRType();
			if(!dbrType.isV3Type()) {
				PVMetrics metricsforPV = ArchiveEngine_EPICSV4.getMetricsforPV(pvName,configService);
				if(metricsforPV != null) {
					String statusStr = new EngineChannelStatus(metricsforPV).toJSONString();
					resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
					try (PrintWriter out = resp.getWriter()) { 
						out.print(statusStr);
					}
				} else {
					logger.warn("Could not determine metrics for PV " + pvName);
					resp.sendError(HttpServletResponse.SC_NOT_FOUND);
				}
			} else {
				PVMetrics metricsforPV = ArchiveEngine.getMetricsforPV(pvName,configService);
				if(metricsforPV != null) {
					String statusStr = new EngineChannelStatus(metricsforPV).toJSONString();
					resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
					try (PrintWriter out = resp.getWriter()) { 
						out.print(statusStr);
					}
				} else {
					logger.warn("Could not determine metrics for PV " + pvName);
					resp.sendError(HttpServletResponse.SC_NOT_FOUND);
				}
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}
	}
}
