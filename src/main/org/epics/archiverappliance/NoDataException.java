/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance;

import java.io.IOException;

/**
 * Thrown when a reader has no data for a particular time frame and particular PV.
 * @author mshankar
 *
 */
@SuppressWarnings("serial")
public class NoDataException extends IOException {
	public NoDataException(String message) {
		super(message);
	}
}
