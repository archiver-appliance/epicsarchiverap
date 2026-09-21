/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common.reports;

import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ConfigService;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Base for the per component {@code /metrics} endpoints.
 *
 * <p>Owns the response envelope every component shares: the appliance label, the content type and
 * writing the collected snapshots out. Subclasses only fill the writer in {@link #collect}, either
 * from a metrics report through {@link DetailsMetricsConverter} or by adding gauges directly.
 *
 * <p>Each war serves its own numbers and Prometheus scrapes it directly, rather than going through
 * the cluster-wide fan-out the mgmt reports perform.
 *
 * @author caraxlr
 */
public abstract class PrometheusExporter implements BPLAction {

    @Override
    public final void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String appliance = configService.getMyApplianceInfo().getIdentity();
        PrometheusMetricsWriter writer = new PrometheusMetricsWriter(Map.of("appliance", appliance));

        collect(writer, configService);

        // Prometheus reads the version out of the content type to pick a parser.
        resp.setContentType(writer.getContentType());
        try (OutputStream out = resp.getOutputStream()) {
            writer.writeTo(out);
        }
    }

    /**
     * Add this component's metrics to the writer.
     *
     * @param writer Collects the samples; already carries the appliance label.
     * @param configService The config service handed to the action.
     * @throws IOException On failure to gather a metric.
     */
    protected abstract void collect(PrometheusMetricsWriter writer, ConfigService configService) throws IOException;
}
