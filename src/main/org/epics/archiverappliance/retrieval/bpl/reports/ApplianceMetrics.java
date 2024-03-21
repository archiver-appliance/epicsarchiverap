/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.bpl.reports;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.retrieval.RetrievalMetrics;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONValue;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Summary metrics for retrieval for an alliance.
 * @author mshankar
 *
 */
public class ApplianceMetrics implements BPLAction {

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println(summedMetricsJsonString(configService));
        }
    }

    private static RetrievalMetrics calculateSummedMetrics(ConfigService configService) {
        var allMetrics = configService.getRetrievalRuntimeState().getRetrievalMetrics();
        return allMetrics.values().stream().reduce(new RetrievalMetrics(), RetrievalMetrics::sumMetrics);
    }

    public static String summedMetricsJsonString(ConfigService configService) {
        return JSONValue.toJSONString(calculateSummedMetrics(configService).getMetrics());
    }
}
