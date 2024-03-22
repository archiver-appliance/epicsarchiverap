/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl.bpl.reports;

import org.epics.archiverappliance.common.reports.Metrics;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.common.ETLMetrics;

import java.util.HashMap;
import java.util.Map;

/**
 * Gets the ETL appliance metrics
 * @author mshankar
 *
 */
public class ApplianceMetrics implements Metrics {

    @Override
    public Map<String, String> metrics(ConfigService configService) {
        HashMap<String, String> metrics = new HashMap<String, String>();
        ETLMetrics metricsForLifetime = configService.getETLLookup().getApplianceMetrics();
        if (metricsForLifetime == null) {
            metrics.put("Startup", "In Progress");
        } else {
            return metricsForLifetime.metrics();
        }

        return metrics;
    }
}
