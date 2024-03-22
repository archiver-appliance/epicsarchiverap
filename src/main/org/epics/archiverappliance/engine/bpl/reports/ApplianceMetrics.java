/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl.reports;

import org.epics.archiverappliance.common.reports.Metrics;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.engine.epics.EngineMetrics;

import java.util.Map;

/**
 * Summary appliance metrics for the engine.
 * @author mshankar
 *
 */
public class ApplianceMetrics implements Metrics {

    @Override
    public Map<String, String> metrics(ConfigService configService) {
        return EngineMetrics.computeEngineMetrics(configService.getEngineContext(), configService)
                .toJSONString();
    }
}
