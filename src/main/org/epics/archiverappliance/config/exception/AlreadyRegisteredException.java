/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config.exception;

import org.epics.archiverappliance.config.ApplianceInfo;

/**
 * Thrown if a PV is already registered to another appliance
 * @author mshankar
 *
 */
public class AlreadyRegisteredException extends ConfigException {
	private static final long serialVersionUID = 2624876692955916732L;
	private ApplianceInfo currentlyRegisteredAppliance;
	public AlreadyRegisteredException(ApplianceInfo currentlyRegisteredAppliance) {
		super();
		this.currentlyRegisteredAppliance = currentlyRegisteredAppliance;
	}
	public ApplianceInfo getCurrentlyRegisteredAppliance() {
		return currentlyRegisteredAppliance;
	}
}
