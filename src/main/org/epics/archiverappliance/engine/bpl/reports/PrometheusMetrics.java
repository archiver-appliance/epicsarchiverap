/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl.reports;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.reports.PrometheusMetricsWriter;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.epics.EngineMetrics;
import org.epics.archiverappliance.engine.metadata.MetaGet;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Engine metrics in the Prometheus text exposition format.
 *
 * <p>Prometheus pulls, so it needs an endpoint per scrape target. Each war is a separate JVM and
 * already knows its own numbers, so each serves its own metrics and Prometheus scrapes it directly.
 * That avoids the cluster-wide fan-out the mgmt reports perform, and it means a war that is down
 * shows up as a down scrape target rather than as a missing field in someone else's report.
 *
 * <p>Unlike the JSON reports, which format every value for display, this writes the underlying
 * numbers. Consumers do not have to parse thousands separators back out of a string.
 *
 * @epics.BPLAction - Return the engine metrics for this appliance in the Prometheus text exposition format. Intended to be scraped by Prometheus rather than read by a person.
 * @epics.BPLActionEnd
 *
 * @author caraxlr
 */
public class PrometheusMetrics implements BPLAction {
    private static final double BYTES_PER_GB = 1024.0 * 1024.0 * 1024.0;
    private static final double SECONDS_PER_DAY = 60.0 * 60.0 * 24.0;

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        EngineMetrics metrics = EngineMetrics.computeEngineMetrics(configService.getEngineContext(), configService);
        String appliance = configService.getMyApplianceInfo().getIdentity();

        PrometheusMetricsWriter writer = new PrometheusMetricsWriter(Map.of("appliance", appliance));

        writer.gauge("pv_count", "PVs assigned to this appliance.", metrics.getPvCount());
        writer.gauge(
                "pv_connected", "PVs the engine currently has a live connection to.", metrics.getConnectedPVCount());
        writer.gauge("pv_disconnected", "PVs the engine has lost the connection to.", metrics.getDisconnectedPVCount());
        writer.gauge("pv_paused", "PVs whose archiving is paused.", metrics.getPausedPVCount());
        writer.gauge(
                "pv_pending_meta_info",
                "PVs whose meta info the engine has not finished computing.",
                MetaGet.getPendingMetaGetsSize());
        writer.gauge("epics_channels", "EPICS channels open for these PVs.", metrics.getTotalEPICSChannels());

        writer.gauge(
                "event_rate_events_per_second", "Events per second arriving at the engine.", metrics.getEventRate());
        // Also published by the JSON reports as GB/day, which is what operators plan capacity in.
        writer.gauge(
                "data_rate_gibibytes_per_day",
                "Gibibytes per day arriving at the engine.",
                metrics.getDataRate() * SECONDS_PER_DAY / BYTES_PER_GB);

        // Prometheus reads the version out of the content type to pick a parser.
        resp.setContentType(writer.getContentType());
        try (OutputStream out = resp.getOutputStream()) {
            writer.writeTo(out);
        }
    }
}
