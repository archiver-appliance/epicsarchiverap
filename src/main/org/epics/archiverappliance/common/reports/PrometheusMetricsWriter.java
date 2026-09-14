/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common.reports;

import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Collects appliance metrics and renders them in the Prometheus text exposition format.
 *
 * <p>The client library handles escaping, value rendering and the HELP/TYPE lines; this class adds
 * the {@link #PREFIX} and the labels common to every sample. Nothing is written until
 * {@link #writeTo}, which sorts the output by name.
 *
 * @author caraxlr
 */
public class PrometheusMetricsWriter {
    /** Metric name prefix for everything the appliance exports. */
    public static final String PREFIX = "archappl_";

    private static final PrometheusTextFormatWriter FORMAT = PrometheusTextFormatWriter.create();

    private final Map<String, String> commonLabels;
    private final MetricSnapshots.Builder snapshots = MetricSnapshots.builder();

    /**
     * @param commonLabels Labels added to every sample, typically the appliance identity.
     */
    public PrometheusMetricsWriter(Map<String, String> commonLabels) {
        this.commonLabels = commonLabels == null ? Map.of() : commonLabels;
    }

    /**
     * Add a gauge: a value that goes up and down, such as a PV count or a rate.
     *
     * @param name Metric name, without the {@link #PREFIX}.
     * @param help One line description.
     * @param value The current reading.
     */
    public void gauge(String name, String help, double value) {
        gauge(name, help, value, Map.of());
    }

    /**
     * Add a gauge carrying extra labels beyond the common ones.
     *
     * @param name Metric name, without the {@link #PREFIX}.
     * @param help One line description.
     * @param value The current reading.
     * @param labels Labels for this sample, merged over the common labels.
     */
    public void gauge(String name, String help, double value, Map<String, String> labels) {
        snapshots.metricSnapshot(GaugeSnapshot.builder()
                .name(PREFIX + name)
                .help(help)
                .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder()
                        .labels(labelsFor(labels))
                        .value(value)
                        .build())
                .build());
    }

    /** The content type to set on the response, including the format version Prometheus parses with. */
    public String getContentType() {
        return FORMAT.getContentType();
    }

    /**
     * Render everything collected so far. The library encodes as UTF-8, so this takes the raw stream
     * rather than the response writer.
     *
     * @param out Destination for the exposition text.
     * @throws IOException On write failure.
     */
    public void writeTo(OutputStream out) throws IOException {
        FORMAT.write(out, snapshots.build());
    }

    private Labels labelsFor(Map<String, String> labels) {
        Map<String, String> merged = new LinkedHashMap<>(commonLabels);
        merged.putAll(labels);
        if (merged.isEmpty()) {
            return Labels.EMPTY;
        }
        return Labels.of(merged.keySet().toArray(new String[0]), merged.values().toArray(new String[0]));
    }
}
