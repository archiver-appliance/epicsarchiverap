/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.pva;


import com.google.protobuf.InvalidProtocolBufferException;
import edu.stanford.slac.archiverappliance.PB.data.PBParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.retrieval.ChangeInYearsException;
import org.epics.archiverappliance.retrieval.EventStreamConsumer;
import org.epics.archiverappliance.retrieval.mimeresponses.ExceptionCommunicator;
import org.epics.pva.data.PVAStructure;

import java.time.Instant;

/**
 * Implementation of the Merge/Dedup algorithm for combining EventStreams into one EventStream.
 * @author mshankar
 *
 */
public class PvaMergeDedupConsumer implements EventStreamConsumer, AutoCloseable {
	private static final Logger logger = LogManager.getLogger(PvaMergeDedupConsumer.class.getName());

    private Instant startTimeStamp;
	int totalEvents = 0;
	int skippedEvents = 0;
	int comparedEvents = 0;
    private Instant timestampOfLastEvent;
	boolean amIDeduping = false;
	boolean haveIpushedTheFirstEvent = false;
	Event firstEvent = null;
	PvaMimeResponse mimeresponse = null;
	String pvName = null;

	int totalEventsForAllPVs = 0;
	int skippedEventsForAllPVs = 0;
	int comparedEventsForAllPVs = 0;

	private final PVAStructure resultStruct;

	PvaMergeDedupConsumer(PvaMimeResponse mimeresponse, PVAStructure resultStruct) {
		this.mimeresponse = mimeresponse;
		this.resultStruct = resultStruct;
		this.mimeresponse.setOutputStruct(resultStruct);
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
				logger.info("Pushing the first event as part of the close as it has not been sent yet.");
				mimeresponse.consumeEvent(firstEvent);
				totalEvents++;
				haveIpushedTheFirstEvent = true;
			} catch(Exception ex) {
				logger.error("Unable to even push the first event thru for pv " + pvName, ex);
			}
		}
		logNumbersAndCollectTotal();
		mimeresponse.close();
	}
	
	public PVAStructure send() {
		return resultStruct;
	}

	/*
	 * Goes through the PB data stream over a period of time. The relevant MIME
	 * response actually deal with the processing of the PV. `start` and `end` refer
	 * to the very beginning and very end of the time period being retrieved over,
	 * regardless of whether it is divided up or not.
	 */
	public void processingPV(BasicContext retrievalContext, String PV, Instant start, Instant end, EventStreamDesc streamDesc) {
		logNumbersAndCollectTotal();
		this.startTimeStamp = start;
		mimeresponse.processingPV(retrievalContext, PV, start, end, streamDesc);
		pvName = PV;
		resetForNextPV();
	}
	
	private void consumeEventStreamAndOutputToMimeResponse(EventStream strm) throws Exception {
		try {
			int eventsInCurrentStream = 0;
			for(Event e : strm) {
				try {
					eventsInCurrentStream++;

					handleEvent(e);
				} catch(InvalidProtocolBufferException|PBParseException ex) {
					logger.warn(ex.getMessage(), ex);
					if(this.mimeresponse instanceof ExceptionCommunicator) {
						((ExceptionCommunicator)this.mimeresponse).comminucateException(ex);
					}
					skippedEvents++;
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

	private void handleEvent(Event e) throws Exception {
		if(!haveIpushedTheFirstEvent && firstEvent == null) {
			logger.debug("Making a copy of the first event " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
			firstEvent = e.makeClone();
			return;
					}
					
					if(!haveIpushedTheFirstEvent) {
                        if (e.getEventTimeStamp().isBefore(this.startTimeStamp)) {
				logger.debug("Making a copy of another event " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
				firstEvent = e.makeClone();
			} else {
				haveIpushedTheFirstEvent = true;
				logger.debug("Consuming first and current events " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
				mimeresponse.consumeEvent(firstEvent);
				timestampOfLastEvent = firstEvent.getEventTimeStamp();
				totalEvents++;
                            if (!e.getEventTimeStamp().isAfter(timestampOfLastEvent)) {
					logger.debug("After sending first event, current event is not after the first event. Skipping " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
					skippedEvents++;
				} else {
					mimeresponse.consumeEvent(e);
					totalEvents++;
					timestampOfLastEvent = e.getEventTimeStamp();
				}
			}
			return;
		}

		if(amIDeduping) {
			comparedEvents++;
                        if (!e.getEventTimeStamp().isAfter(timestampOfLastEvent)) {
				skippedEvents++;
			} else {
				amIDeduping = false;
				mimeresponse.consumeEvent(e);
				timestampOfLastEvent = e.getEventTimeStamp();
				totalEvents++;
			}
		} else {
			mimeresponse.consumeEvent(e);
			timestampOfLastEvent = e.getEventTimeStamp();
			totalEvents++;
		}
	}


	private void startDeduping() {
		amIDeduping = true;
	}
	
	public void resetForNextPV() {
		totalEvents = 0;
		skippedEvents = 0;
		comparedEvents = 0;
        timestampOfLastEvent = Instant.ofEpochMilli(Long.MIN_VALUE);
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