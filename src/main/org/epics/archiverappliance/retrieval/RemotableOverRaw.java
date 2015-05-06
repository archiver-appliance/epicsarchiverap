/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

/**
 * This is applicable to EventStreams.
 * If we want an EventStream to be remotable over the RAW response mime type, then we need this additional info for the PB header.
 * @author mshankar
 *
 */
public interface RemotableOverRaw {
	public RemotableEventStreamDesc getDescription();
}
