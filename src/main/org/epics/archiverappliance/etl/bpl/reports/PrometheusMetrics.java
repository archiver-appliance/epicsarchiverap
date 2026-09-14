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
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.common.reports.PrometheusMetricsWriter;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.common.ETLMetricsIntoStore;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Storage metrics for this appliance's stores in the Prometheus text exposition format.
 *
 * <p>ETL owns the stores, so it serves this rather than the engine. Prometheus scrapes each war
 * directly; nothing here calls out to another component, so an outage elsewhere cannot fail this
 * scrape.
 *
 * <p>Only whole-store space is reported. Per-PV storage is deliberately absent: computing it globs
 * the filesystem for every PV in every store, which is far too expensive to sit behind a scrape.
 * Use getPVsByStorageConsumed when you need that.
 *
 * @epics.BPLAction - Return the storage metrics for this appliance in the Prometheus text exposition format. Intended to be scraped by Prometheus rather than read by a person.
 * @epics.BPLActionEnd
 *
 * @author caraxlr
 */
public class PrometheusMetrics implements BPLAction {
    private static final Logger logger = LogManager.getLogger(PrometheusMetrics.class);

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String appliance = configService.getMyApplianceInfo().getIdentity();
        PrometheusMetricsWriter writer = new PrometheusMetricsWriter(Map.of("appliance", appliance));

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
                // Derivable from the two above, but exported so alerts can name a threshold directly
                // and so this matches the percentage the storage report shows.
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

        // Prometheus reads the version out of the content type to pick a parser.
        resp.setContentType(writer.getContentType());
        try (OutputStream out = resp.getOutputStream()) {
            writer.writeTo(out);
        }
    }
}
