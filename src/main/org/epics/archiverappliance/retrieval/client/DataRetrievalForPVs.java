/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.Reader;

import java.time.Instant;
import java.util.HashMap;

/**
 * Similar to the reader interface, this is the main retrieval interface.
 * The main implementation is over a PB over HTTP .raw call to the server.  
 * This is a streaming interface in that data for multiple PVs are returned as part of the same EventStream.
 * Out-of-band events (such as transitions into another PV's data) are passed as events on the RetrievalEventProcessor.
 * 
 * @author mshankar
 * @see org.epics.archiverappliance.retrieval.DataRetrievalServlet
 * @see Reader
 */
public interface DataRetrievalForPVs {
	/**
	 * Get data for multiple PVs from starttime to endtime.
	 * Pass out-of-band events into the supplied retrievalEventProcessor
	 * We default to getting raw data
	 * @param pvNames The name of PVs
     * @param startTime  Instant
     * @param endTime  Instant
	 * @param retrievalEventProcessor RetrievalEventProcessor
	 * @return EventStream  Data for PVs &emsp; 
	 */
    public EventStream getDataForPVS(String[] pvNames, Instant startTime, Instant endTime, RetrievalEventProcessor retrievalEventProcessor);
	/**
	 * Get data for multiple PVs from starttime to endtime.
	 * Pass out-of-band events into the supplied retrievalEventProcessor
	 * @param pvNames The name of PVs
     * @param startTime  Instant
     * @param endTime  Instant
	 * @param retrievalEventProcessor RetrievalEventProcessor
	 * @param useReducedDataSet Is it ok to use a reduced data set?
	 * @return EventStream Data for PVs
	 */
    public EventStream getDataForPVS(String[] pvNames, Instant startTime, Instant endTime, RetrievalEventProcessor retrievalEventProcessor, boolean useReducedDataSet);

	/**
	 * Get data for multiple PVs from starttime to endtime.
	 * Pass out-of-band events into the supplied retrievalEventProcessor
	 * @param pvNames The name of PVs
     * @param startTime  Instant
     * @param endTime  Instant
	 * @param retrievalEventProcessor RetrievalEventProcessor
	 * @param useReducedDataSet Is it ok to use a reduced data set?
	 * @param otherParams Any other name/value pairs that are passed onto the server. 
	 * @return EventStream Data for PVs
	 */
    public EventStream getDataForPVS(String[] pvNames, Instant startTime, Instant endTime, RetrievalEventProcessor retrievalEventProcessor, boolean useReducedDataSet, HashMap<String, String> otherParams);
}
