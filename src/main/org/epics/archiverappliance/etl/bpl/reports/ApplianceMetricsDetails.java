/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl.bpl.reports;

import org.epics.archiverappliance.common.reports.MetricsDetails;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.common.ETLMetrics;

import java.util.LinkedList;
import java.util.Map;

/**
 * Get the metrics details for an appliance for ETL.
 * @author mshankar
 *
 */
public class ApplianceMetricsDetails implements MetricsDetails {

    @Override
    public LinkedList<Map<String, String>> metricsDetails(ConfigService configService) {
        ETLMetrics etlMetrics = configService.getETLLookup().getApplianceMetrics();
        return etlMetrics.details(configService);
    }
}
