/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl.bpl.reports;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.etl.common.ETLDetails;

import java.util.LinkedList;
import java.util.Map;

/**
 * Gets the ETL details of a PV.
 * @author mshankar
 *
 */
public class PVDetails implements org.epics.archiverappliance.common.reports.PVDetails {

    @Override
    public LinkedList<Map<String, String>> pvDetails(ConfigService configService, String pvName) throws Exception {
        ETLDetails details = new ETLDetails(pvName);
        return details.details(configService);
    }
}
