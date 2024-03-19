/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

import org.epics.archiverappliance.EventStream;

/**
 * Result of a retrieval.
 * This is mostly a EventStream, in addition it also has a reference to the original UnitOfRetrieval mostly for logging purposes.
 * @author mshankar
 *
 */
public class RetrievalResult {
	private List<Callable<EventStream>> resultStreams;
	private UnitOfRetrieval retrievalRequest;
	
	public List<Callable<EventStream>> getResultStreams() {
		return resultStreams;
	}
	
	public UnitOfRetrieval getRetrievalRequest() {
		return retrievalRequest;
	}
	
	public RetrievalResult(List<Callable<EventStream>> strms, UnitOfRetrieval retrievalRequest) throws IOException {
		this.resultStreams = strms;
		this.retrievalRequest = retrievalRequest;
	}
	
	public boolean hasNoData() {
		if(resultStreams == null) return true;
		
		return false;
	}
}
