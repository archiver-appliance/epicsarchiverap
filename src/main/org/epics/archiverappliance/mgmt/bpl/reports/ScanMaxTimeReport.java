/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl.reports;

import org.epics.archiverappliance.config.ApplianceInfo;

/**
 * Scan copy time report
 * 
 * @epics.BPLAction - Return a list of PVs sorted by the maximum time between calls of the SCAN thread (descending). The engine establishes tasks on a ScheduledThreadExecutor for SCANning. There is no guarantee that these will happen in the specified sampling period; use this report to tailor the number of SCAN threads.    
 * @epics.BPLActionParam limit - Limit this report to this many PVs per appliance in the cluster. Optional, if unspecified, there are no limits enforced.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class ScanMaxTimeReport extends GenericMultiApplianceReport {	
	public ScanMaxTimeReport() { 
		super(ApplianceInfo::getEngineURL, "/getPVsByMaxTimeBetweenScans", ScanMaxTimeReport.class.getName());
	}
}
