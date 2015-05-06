/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config.ChannelArchiver;

/**
 * Represents the configuration for a single PV in the engine config file.
 * 
 * @author mshankar
 *
 */
public class PVConfig {
	private String PVName;
	private float samplingPeriod;
	private boolean monitor;
	
	public PVConfig(String pVName, float period, boolean monitor) {
		super();
		PVName = pVName;
		this.samplingPeriod = period;
		this.monitor = monitor;
	}

	public String getPVName() {
		return PVName;
	}

	public float getPeriod() {
		return samplingPeriod;
	}

	public boolean isMonitor() {
		return monitor;
	}

	@Override
	public String toString() {
		return "PV: " + PVName + " is being " + (monitor ? "monitored" : "scanned") + " with period " + samplingPeriod + "(s)";
	}
}
