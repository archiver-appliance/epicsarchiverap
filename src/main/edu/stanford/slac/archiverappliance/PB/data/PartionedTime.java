/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import org.epics.archiverappliance.common.YearSecondTimestamp;

/**
 * All the PB classes also implement this version of timeinfo.
 * @author mshankar
 *
 */
public interface PartionedTime {
	YearSecondTimestamp getYearSecondTimestamp();
}
