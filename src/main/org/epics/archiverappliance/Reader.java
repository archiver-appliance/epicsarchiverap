/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance;

import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * The main data retrieval interface; this is used to get an EventStream for events for one PV between a start and an end time.
 * If a sample does not exist between these intervals, we'll need to return the last known value
 * The reader interface is almost always used as part of the storage plugin interfaces.
 * @author mshankar
 *
 */
public interface Reader {
	List<Callable<EventStream>> getDataForPV(BasicContext context, String pvName, Instant startTime, Instant endTime, PostProcessor postProcessor) throws IOException;

	/**
	 * Get the first event for this PV.
	 * This call is used to optimize away calls to other readers that have older data.
	 * @param context  &emsp;
	 * @param pvName The PV name 
	 * @return Event The first event of pvName
	 * @throws IOException  &emsp;
	 */
	public Event getFirstKnownEvent(BasicContext context, String pvName) throws IOException;
}
