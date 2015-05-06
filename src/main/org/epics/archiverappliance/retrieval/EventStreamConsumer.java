/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

import org.epics.archiverappliance.EventStream;

/**
 * @author mshankar
 *
 * An interface for an entity that consumes events from a stream all at once.
 */
public interface EventStreamConsumer {
	public void consumeEventStream(EventStream e) throws Exception;
}
