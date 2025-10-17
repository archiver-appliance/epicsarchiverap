/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.epics.EngineChannelStatus;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * BPL for getting the status of a PV.
 * @author mshankar
 *
 */
public class PVStatusAction implements BPLAction {
    private static Logger logger = LogManager.getLogger(PVStatusAction.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String pvNamesStr = req.getParameter("pv");
        if (pvNamesStr == null || pvNamesStr.equals("")) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        String[] pvNames = pvNamesStr.split(",");
        LinkedList<EngineChannelStatus> statuses = new LinkedList<EngineChannelStatus>();

        for (String pvName : pvNames) {
            try {
                PVTypeInfo typeInfoForPV = configService.getTypeInfoForPV(pvName);
                if (typeInfoForPV == null) {
                    logger.error("Could not find pv type info for PV " + pvName);
                    continue;
                }
                PVMetrics metricsforPV = ArchiveEngine.getMetricsforPV(pvName, configService);
                if (metricsforPV != null) {
                    statuses.add(new EngineChannelStatus(metricsforPV));
                } else {
                    logger.warn("Could not determine metrics for PV " + pvName);
                    continue;
                }
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        }

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        boolean first = true;
        try (PrintWriter out = resp.getWriter()) {
            out.println("[");
            for (EngineChannelStatus status : statuses) {
                if (first) {
                    first = false;
                } else {
                    out.println(",");
                }
                out.print(status.toJSONString());
            }
            out.println();
            out.println("]");
        }
    }
}
