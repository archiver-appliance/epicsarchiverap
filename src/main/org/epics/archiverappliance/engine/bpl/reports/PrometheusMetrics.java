/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl.reports;

import org.epics.archiverappliance.common.reports.DetailsMetricsConverter;
import org.epics.archiverappliance.common.reports.PrometheusExporter;
import org.epics.archiverappliance.common.reports.PrometheusMetricsWriter;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.epics.EngineMetrics;

/**
 * Engine metrics in the Prometheus text exposition format.
 *
 * <p>Publishes the numeric entries of {@link EngineMetrics#details}, so an entry added there is
 * exported without touching this class.
 *
 * @epics.BPLAction - Return the engine metrics for this appliance in the Prometheus text exposition format. Intended to be scraped by Prometheus rather than read by a person.
 * @epics.BPLActionEnd
 *
 * @author caraxlr
 */
public class PrometheusMetrics extends PrometheusExporter {

    @Override
    protected void collect(PrometheusMetricsWriter writer, ConfigService configService) {
        EngineMetrics metrics = EngineMetrics.computeEngineMetrics(configService.getEngineContext(), configService);
        DetailsMetricsConverter.addAll(writer, metrics.details(configService));
    }
}
