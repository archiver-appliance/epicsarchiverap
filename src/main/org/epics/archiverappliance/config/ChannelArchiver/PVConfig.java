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
	private String policy;
	
	public PVConfig(String pVName, float period, boolean monitor, String policy) {
		super();
		PVName = pVName;
		this.samplingPeriod = period;
		this.monitor = monitor;
		this.policy = policy;
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

	public String getPolicy() {
		return policy;
	}

	@Override
	public String toString() {
		String result = "PV: " + PVName + " is being " + (monitor ? "monitored" : "scanned") + " with period " + samplingPeriod + "(s)";
		if (policy != null) {
			result = result + " and policy " + policy;
		}
		return result;
	}
}
