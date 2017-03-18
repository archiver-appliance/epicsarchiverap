/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.bpl.reports;

import org.epics.archiverappliance.engine.pv.PVMetrics;

/**
 * Sort by the time taken by the scan thread to write the sample into the buffer.
 * @author mshankar
 *
 */
public class ScanMaxTimeReport extends GenericPVMetricsReport<Long> {
	public ScanMaxTimeReport() { 
		super(PVMetrics::getScanProcessingTime, "maxTimeBetweenScans");
	}
}
