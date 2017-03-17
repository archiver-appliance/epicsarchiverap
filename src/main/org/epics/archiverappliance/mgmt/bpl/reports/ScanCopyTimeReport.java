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
 * @epics.BPLAction - Return a list of PVs sorted by descending scan copy times. 
 * @epics.BPLActionParam limit - Limit this report to this many PVs per appliance in the cluster. Optional, if unspecified, there are no limits enforced.
 * @epics.BPLActionEnd
 * 
 * @author mshankar
 *
 */
public class ScanCopyTimeReport extends GenericMultiApplianceReport {	
	public ScanCopyTimeReport() { 
		super(ApplianceInfo::getEngineURL, "/getPVsByScanCopyTime", ScanCopyTimeReport.class.getName());
	}
}
