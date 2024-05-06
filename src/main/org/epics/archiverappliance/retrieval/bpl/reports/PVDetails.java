/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.bpl.reports;

import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.retrieval.RetrievalMetrics;

import java.util.LinkedList;
import java.util.Map;

/**
 * Gets the Retrieval details of a PV.
 *
 */
public class PVDetails implements org.epics.archiverappliance.common.reports.PVDetails {

    @Override
    public LinkedList<Map<String, String>> pvDetails(ConfigService configService, String pvName) throws Exception {
        RetrievalMetrics retrievalMetrics =
                configService.getRetrievalRuntimeState().getPVRetrievalMetrics(pvName);
        if (retrievalMetrics == null) retrievalMetrics = RetrievalMetrics.EMPTY_METRICS;
        return new LinkedList<>(retrievalMetrics.details(configService));
    }
}
