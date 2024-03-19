/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.data;

/**
 * Get the status and severity of this event.
 * @author mshankar
 */
public interface AlarmInfo {
	public int getStatus();
	public int getSeverity();
	public void setStatus(int status);
	public void setSeverity(int severity);
}
