/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.epics;

import org.epics.archiverappliance.common.reports.Details;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.ConfigService.WAR_FILE;
import org.epics.archiverappliance.engine.pv.EngineContext;

import java.util.LinkedList;
import java.util.Map;
/**
 * POJO with some basic metrics.
 * @author mshankar
 *
 */
public class CommandThreadDetails implements Details {

    @Override
    public LinkedList<Map<String, String>> details(ConfigService configService) {
        LinkedList<Map<String, String>> details = new LinkedList<Map<String, String>>();
        EngineContext context = configService.getEngineContext();
        details.addAll(context.getCommandThreadDetails());
        return details;
    }

    @Override
    public WAR_FILE source() {
        return ConfigService.WAR_FILE.ENGINE;
    }
}
