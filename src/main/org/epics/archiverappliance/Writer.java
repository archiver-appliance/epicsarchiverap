/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance;

import java.io.IOException;

import org.epics.archiverappliance.common.BasicContext;

/**
 * The main data consumption interface, this represents objects which take an event stream and write it to a (potentially permanent) store.
 * @author mshankar
 *
 */
public interface Writer {
	boolean appendData(BasicContext context, String pvName, EventStream stream) throws IOException;

	/**
	 * Gets the last known event in this destination. 
	 * Future events will be appended to this destination only if their timestamp is more recent than the timestamp of this event.
	 * If there is no last known event, then a null is returned.
	 * @param pvName
	 * @return
	 * @throws IOException
	 */
	public Event getLastKnownEvent(BasicContext context, String pvName) throws IOException;
}
