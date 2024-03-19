/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.retrieval.RetrievalMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Gets the ETL details of a PV.
 * @author mshankar
 *
 */
public class PVDetails implements BPLAction {
    private static final Logger logger = LogManager.getLogger(PVDetails.class);

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String pvName = req.getParameter("pv");
        if (pvName == null || pvName.isEmpty()) {
            resp.sendError(HttpServletResponse.SC_BAD_REQUEST);
            return;
        }

        logger.info("Getting the detailed status for PV " + pvName);
        String detailedStatus = getRetrievalMetricsString(configService, pvName);
        if (detailedStatus != null) {
            resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
            try (PrintWriter out = resp.getWriter()) {
                out.print(detailedStatus);
            }
        } else {
            logger.debug("No status for PV " + pvName + " in this ETL.");
            resp.sendError(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
    }

    private static void addDetailedStatus(LinkedList<Map<String, String>> statuses, String name, String value) {
        Map<String, String> obj = new LinkedHashMap<String, String>();
        obj.put("name", name);
        obj.put("value", value);
        obj.put("source", "retrieval");
        statuses.add(obj);
    }

    private String getRetrievalMetricsString(ConfigService configService, String pvName) {
        LinkedList<Map<String, String>> statuses = new LinkedList<Map<String, String>>();
        addDetailedStatus(statuses, "Name (from Retrieval)", pvName);
        RetrievalMetrics retrievalMetrics =
                configService.getRetrievalRuntimeState().getPVRetrievalMetrics(pvName);
        if (retrievalMetrics == null) retrievalMetrics = RetrievalMetrics.EMPTY_METRICS;
        statuses.addAll(retrievalMetrics.getDetails());

        return JSONValue.toJSONString(statuses);
    }
}
