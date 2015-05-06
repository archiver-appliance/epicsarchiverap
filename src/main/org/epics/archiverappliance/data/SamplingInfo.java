/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.data;

/**
 * Some additional info pertaining to the process of archiving. 
 * For now, we only include the count. 
 * @author mshankar
 */
public interface SamplingInfo {
	int getRepeatCount();
	void setRepeatCount(int repeatCount);
}
