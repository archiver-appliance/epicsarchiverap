/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.reports;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.reports.PrometheusMetricsWriter;
import org.epics.archiverappliance.config.ConfigService;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Configuration level PV counts in the Prometheus text exposition format.
 *
 * @epics.BPLAction - Return the mgmt metrics for this appliance in the Prometheus text exposition format. Intended to be scraped by Prometheus rather than read by a person.
 * @epics.BPLActionEnd
 *
 * @author caraxlr
 */
public class PrometheusMetrics implements BPLAction {

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        ConfigService.CachedPVCounts pvCounts = configService.getCachedPVCountsForThisAppliance();
        String appliance = configService.getMyApplianceInfo().getIdentity();

        PrometheusMetricsWriter writer = new PrometheusMetricsWriter(Map.of("appliance", appliance));

        // Cached here in mgmt; reporting it from the engine would cost a cluster call per scrape.
        writer.gauge("pv_paused", "PVs whose archiving is paused.", pvCounts.pausedPVCount());

        // Prometheus reads the version out of the content type to pick a parser.
        resp.setContentType(writer.getContentType());
        try (OutputStream out = resp.getOutputStream()) {
            writer.writeTo(out);
        }
    }
}
