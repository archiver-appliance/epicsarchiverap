/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config.exception;

/**
 * @author mshankar
 * Generic super class for all config service exceptions.
 */
public class ConfigException extends Exception {
	private static final long serialVersionUID = -1195048537953477832L;
	public ConfigException() {
	}
	
	public ConfigException(String msg) {
		super(msg);
	}
	public ConfigException(String msg, Throwable ex) {
		super(msg, ex);
	}
}
