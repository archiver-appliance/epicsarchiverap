/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.reports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.reports.Metrics;
import org.epics.archiverappliance.config.ConfigService;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Get the metrics for this management instance. Typically called from other reports
 * @author mshankar
 *
 */
public class MgmtMetrics implements Metrics {
    private static final Logger logger = LogManager.getLogger(MgmtMetrics.class);

    @Override
    public Map<String, String> metrics(ConfigService configService) {
        Map<String, String> result = new HashMap<String, String>();
        long vmStartTime = ManagementFactory.getRuntimeMXBean().getStartTime();
        Duration vmInterval =
                Duration.between(Instant.ofEpochMilli(vmStartTime), Instant.ofEpochMilli(System.currentTimeMillis()));
        result.put("uptime", vmInterval.toString());
        return result;
    }
}
