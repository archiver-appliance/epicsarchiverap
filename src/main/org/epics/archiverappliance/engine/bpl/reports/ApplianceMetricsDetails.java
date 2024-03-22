/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl.reports;

import org.epics.archiverappliance.common.reports.MetricsDetails;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.epics.EngineMetrics;

import java.util.LinkedList;
import java.util.Map;

/**
 * Detailed appliance metrics for the engine.
 * @author mshankar
 *
 */
public class ApplianceMetricsDetails implements MetricsDetails {

    @Override
    public LinkedList<Map<String, String>> metricsDetails(ConfigService configService) {
        return EngineMetrics.computeEngineMetrics(configService.getEngineContext(), configService)
                .details(configService);
    }
}
