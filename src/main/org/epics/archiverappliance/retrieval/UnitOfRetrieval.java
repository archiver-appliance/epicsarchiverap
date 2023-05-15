/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;


import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.NoDataException;
import org.epics.archiverappliance.Reader;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.mergededup.MergeDedupWithCallablesEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;

/**
 * @author mshankar
 * This class encapsulates a storage plugin, a PVName, a start time and an end time (all the items needed to make a getDataForPV call).
 * The retrieval servlet creates multiple UnitOfRetrieval's, one or more for each storage plugin.
 * Some variation of a ExecutorService is expected to be used to launch the UnitOfRetrievals in parallel and then push the event streams into the consumer.
 */
public class UnitOfRetrieval implements Callable<RetrievalResult> {
	private static final Logger logger = LogManager.getLogger(UnitOfRetrieval.class.getName());
	private final String description;
	private final Reader reader;
	private final String pvName;
	private final String pvNameFromRequest;
	private final Timestamp start;
	private final Timestamp end;
	private final PostProcessor postProcessor;
	private final BasicContext context;
	private List<Callable<EventStream>> failoverStrms;

	
	public UnitOfRetrieval(String desc, Reader reader, String pvName, String pvNameFromRequest, Timestamp start, Timestamp end, PostProcessor postProcessor, BasicContext context) {
		this.description = desc;
		this.reader = reader;
		this.pvName = pvName;
		this.pvNameFromRequest = pvNameFromRequest;
		this.start = start;
		this.end = end;
		this.postProcessor = postProcessor;
		this.context = context;
	}


	@Override
	public RetrievalResult call() throws IOException {
		try {
			logger.debug("Starting get Data for " + pvName + " from " + description);
			List<Callable<EventStream>> strms = reader.getDataForPV(context, pvName, start, end, postProcessor);
			logger.debug("Done getting data for " + pvName + " from " + description);
			if(strms != null && failoverStrms != null) {
				logger.debug("Wrapping and merging retrieval with failover data for " + this.pvName);
				MergeDedupWithCallablesEventStream mret = new MergeDedupWithCallablesEventStream(strms, failoverStrms);
				return new RetrievalResult(CallableEventStream.makeOneStreamCallableList(mret), this);
			} else if(strms != null) {
				logger.debug("Returning only local streams for " + this.pvName);
				return new RetrievalResult(strms, this);
			} else if(failoverStrms != null) {
				logger.debug("Returning only failover streams for " + this.pvName);
				return new RetrievalResult(failoverStrms, this);
			} else {
				logger.error("No data for " + this.pvName);
				return new RetrievalResult(null, this);
			}
		} catch(NoDataException ex) {
			logger.debug("No data from " + description + " " + ex.getMessage());
			return new RetrievalResult(null, this);
		}
	}


	public String getDescription() {
		return description;
	}


	public String getPvName() {
		return pvName;
	}


	public Timestamp getStart() {
		return start;
	}


	public Timestamp getEnd() {
		return end;
	}


	public String getPvNameFromRequest() {
		return pvNameFromRequest;
	}
	
	public void wrapWithFailoverStreams(List<Callable<EventStream>> failoverStrms) {
		this.failoverStrms = failoverStrms;
	}
}
