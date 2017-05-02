/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;

import java.sql.Timestamp;
import java.util.HashMap;

import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.Reader;

/**
 * Similar to the reader interface, this is the main retrieval interface.
 * The main implementation is over a PB over HTTP .raw call to the server.  
 * This is a streaming interface in that data for multiple PVs are returned as part of the same EventStream.
 * Out-of-band events (such as transitions into another PV's data) are passed as events on the RetrievalEventProcessor.
 * 
 * @author mshankar
 * @see DataRetrieval
 * @see Reader
 */
public interface DataRetrieval {
	/**
	 * Get data for multiple PVs from starttime to endtime.
	 * Pass out-of-band events into the supplied retrievalEventProcessor
	 * We default to getting raw data
	 * @param pvNames The name of PVs
	 * @param startTime  Timestamp
	 * @param endTime  Timestamp
	 * @param retrievalEventProcessor RetrievalEventProcessor
	 * @return EventStream  Data for PVs &emsp; 
	 */
	public EventStream getDataForPVS(String[] pvNames, Timestamp startTime, Timestamp endTime, RetrievalEventProcessor retrievalEventProcessor);
	/**
	 * Get data for multiple PVs from starttime to endtime.
	 * Pass out-of-band events into the supplied retrievalEventProcessor
	 * @param pvNames The name of PVs
	 * @param startTime  Timestamp
	 * @param endTime  Timestamp
	 * @param retrievalEventProcessor RetrievalEventProcessor
	 * @param useReducedDataSet Is it ok to use a reduced data set?
	 * @return EventStream Data for PVs
	 */
	public EventStream getDataForPVS(String[] pvNames, Timestamp startTime, Timestamp endTime, RetrievalEventProcessor retrievalEventProcessor, boolean useReducedDataSet);

	/**
	 * Get data for multiple PVs from starttime to endtime.
	 * Pass out-of-band events into the supplied retrievalEventProcessor
	 * @param pvNames The name of PVs
	 * @param startTime  Timestamp
	 * @param endTime  Timestamp
	 * @param retrievalEventProcessor RetrievalEventProcessor
	 * @param useReducedDataSet Is it ok to use a reduced data set?
	 * @param otherParams Any other name/value pairs that are passed onto the server. 
	 * @return EventStream Data for PVs
	 */
	public EventStream getDataForPVS(String[] pvNames, Timestamp startTime, Timestamp endTime, RetrievalEventProcessor retrievalEventProcessor, boolean useReducedDataSet, HashMap<String, String> otherParams);
}
