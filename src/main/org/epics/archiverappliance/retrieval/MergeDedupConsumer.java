/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;


import java.io.OutputStream;
import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.retrieval.mimeresponses.ExceptionCommunicator;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;

import com.google.protobuf.InvalidProtocolBufferException;

import edu.stanford.slac.archiverappliance.PB.data.PBParseException;

/**
 * Implementation of the Merge/Dedup algorithm for combining EventStreams into one EventStream.
 * @author mshankar
 *
 */
class MergeDedupConsumer implements EventStreamConsumer, AutoCloseable {
	private static Logger logger = Logger.getLogger(MergeDedupConsumer.class.getName());
	private Timestamp startTimeStamp;
	int totalEvents = 0;
	int skippedEvents = 0;
	int comparedEvents = 0;
	OutputStream os = null;
	long epochSecondsOfLastEventInPreviousStream = 0;
	boolean amIDeduping = false;
	boolean haveIpushedTheFirstEvent = false;
	Event firstEvent = null;
	MimeResponse mimeresponse = null;
	String pvName = null;

	int totalEventsForAllPVs = 0;
	int skippedEventsForAllPVs = 0;
	int comparedEventsForAllPVs = 0;
	
	
	MergeDedupConsumer(MimeResponse mimeresponse, OutputStream os) {
		this.os = os;
		this.mimeresponse = mimeresponse;
		this.mimeresponse.setOutputStream(os);
	}
	
	
	@Override
	public void consumeEventStream(EventStream strm) throws Exception {
		// This while true looks scary but 99.99% of the time we should hit the return and get out with one execution of the loop
		// In cases where the data spans year boundaries, we continue with the same stream.
		while(true) {
			try {
				mimeresponse.swicthingToStream(strm);
				consumeEventStreamAndOutputToMimeResponse(strm);
				return;
			} catch(ChangeInYearsException ex) {
				if(logger.isDebugEnabled()) logger.debug("Got a change in years exception. Inserting a fake swicthingToStream with the same stream. Previous=" + ex.getPreviousYear() + " and current=" + ex.getCurrentYear());
			}
		}
	}
	
	@Override
	public void close() {
		if(!haveIpushedTheFirstEvent && firstEvent != null) {
			try {
				logger.debug("Pushing the first event as part of the close as it has not been sent yet.");
				mimeresponse.consumeEvent(firstEvent);
				totalEvents++;
				haveIpushedTheFirstEvent = true;
			} catch(Exception ex) {
				logger.error("Unable to even push the first event thru for pv " + pvName, ex);
			}
		}
		logNumbersAndCollectTotal();
		mimeresponse.close();
		try { 
			os.flush();
		} catch(Throwable t) { 
			logger.debug("Exception flushing response", t);
		}
		
		try { 
			os.close();
		} catch(Throwable t) { 
			logger.debug("Exception closing response", t);
		}
	}
	
	public void processingPV(String PV, Timestamp start, Timestamp end, EventStreamDesc streamDesc) {
		logNumbersAndCollectTotal();
		this.startTimeStamp = start;
		mimeresponse.processingPV(PV, start, end, streamDesc);
		pvName = PV;
		resetForNextPV();
	}
	
	private void consumeEventStreamAndOutputToMimeResponse(EventStream strm) throws Exception {
		try {
			int eventsInCurrentStream = 0;
			for(Event e : strm) {
				eventsInCurrentStream++;
				
				if(!haveIpushedTheFirstEvent && firstEvent == null) {
					logger.debug("Making a copy of the first event " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
					firstEvent = e.makeClone();
					continue;
				}
				
				if(!haveIpushedTheFirstEvent) { 
					if(e.getEventTimeStamp().before(this.startTimeStamp)) {
						logger.debug("Making a copy of another event " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
						firstEvent = e.makeClone();
						continue;
					} else if(e.getEventTimeStamp().equals(this.startTimeStamp)) { 
						logger.debug("Skipping another first event with the same timestamp " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
						continue;
					} else { 
						haveIpushedTheFirstEvent = true;
						logger.debug("Consuming first and current events " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
						mimeresponse.consumeEvent(firstEvent);
						totalEvents++;
						mimeresponse.consumeEvent(e);
						totalEvents++;
						epochSecondsOfLastEventInPreviousStream = e.getEpochSeconds();
						continue;
					}
				}
				
				if(amIDeduping) {
					comparedEvents++;
					long currentEpochSeconds = e.getEpochSeconds(); 
					if(currentEpochSeconds <= epochSecondsOfLastEventInPreviousStream) {
						skippedEvents++;
						continue;
					} else {
						amIDeduping = false;
						mimeresponse.consumeEvent(e);
						epochSecondsOfLastEventInPreviousStream = e.getEpochSeconds();
						totalEvents++;
					}
				} else {
					mimeresponse.consumeEvent(e);
					epochSecondsOfLastEventInPreviousStream = e.getEpochSeconds();
					totalEvents++;
				}
			}
			
			if(eventsInCurrentStream == 0) {
				logger.info("The stream from " + ((strm.getDescription() != null ) ? strm.getDescription().getSource() : "Unknown") + " was an empty stream.");
			}

			// We start deduping at the boundaries of event streams. 
			// This does not apply until we have pushed the first event out..
			if(haveIpushedTheFirstEvent) startDeduping();
		} catch(InvalidProtocolBufferException|PBParseException ex) {
			logger.warn(ex.getMessage(), ex);
			if(this.mimeresponse instanceof ExceptionCommunicator) { 
				((ExceptionCommunicator)this.mimeresponse).comminucateException(ex);
			}
			skippedEvents++;
		}
	}

	
	private void startDeduping() {
		amIDeduping = true;
	}
	
	public void resetForNextPV() {
		totalEvents = 0;
		skippedEvents = 0;
		comparedEvents = 0;
		epochSecondsOfLastEventInPreviousStream = 0;
		amIDeduping = false;
		firstEvent = null;
		haveIpushedTheFirstEvent = false;
	}
	
	public void logNumbersAndCollectTotal() {
		if(pvName != null) {
			logger.info("Found a total of " + totalEvents 
					+ " skipping " + skippedEvents + " events"
					+ " deduping involved " + comparedEvents + " compares for PV "
					+ pvName);
		}
		totalEventsForAllPVs += totalEvents;
		skippedEventsForAllPVs += skippedEvents;
		comparedEventsForAllPVs += comparedEvents;
	}	
}