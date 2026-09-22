/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.reports.PrometheusExporter;
import org.epics.archiverappliance.common.reports.PrometheusMetricsWriter;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.common.ETLMetricsIntoStore;

import java.io.IOException;
import java.util.Map;

/**
 * Storage metrics for this appliance's stores in the Prometheus text exposition format.
 *
 * @epics.BPLAction - Return the storage metrics for this appliance in the Prometheus text exposition format. Intended to be scraped by Prometheus rather than read by a person.
 * @epics.BPLActionEnd
 *
 * @author caraxlr
 */
public class PrometheusMetrics extends PrometheusExporter {
    private static final Logger logger = LogManager.getLogger(PrometheusMetrics.class);

    @Override
    protected void collect(PrometheusMetricsWriter writer, ConfigService configService) {
        for (StorageWithLifetime store : StorageWithLifetime.getStorageWithLifetimes(configService)) {
            String storeName = store.storageName;
            ETLMetricsIntoStore context =
                    configService.getETLLookup().getApplianceMetrics().get(storeName);
            if (context == null) {
                // The context carries the free space cache; without it the plugin cannot answer.
                logger.warn("No ETL metrics yet for store {}; skipping its storage metrics", storeName);
                continue;
            }
            Map<String, String> labels = Map.of("store", storeName);
            try {
                long totalSpace = store.storageMetricsAPI.getTotalSpace(context);
                long usableSpace = store.storageMetricsAPI.getUsableSpace(context);
                writer.gauge("store_total_bytes", "Capacity of the file store backing this stage.", totalSpace, labels);
                writer.gauge(
                        "store_usable_bytes",
                        "Space still available on the file store backing this stage.",
                        usableSpace,
                        labels);
                if (totalSpace > 0) {
                    writer.gauge(
                            "store_usable_ratio",
                            "Free space on this stage as a fraction of its capacity, 0 to 1.",
                            (double) usableSpace / totalSpace,
                            labels);
                }
            } catch (IOException ex) {
                // One unreachable store should not cost us the metrics for the others.
                logger.warn("Exception retrieving storage metrics from " + storeName, ex);
            }
        }
    }
}
