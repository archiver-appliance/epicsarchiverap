/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PBOverHTTP;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.sql.Timestamp;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.client.RetrievalEventProcessor;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;

/**
 * The iterator for the InputStreamBackedEventStream
 * @author mshankar
 *
 */
public class InputStreamBackedEventStreamIterator implements Iterator<Event> {
	private static Logger logger = Logger.getLogger(InputStreamBackedEventStreamIterator.class.getName());
	private static int BUFFER_SIZE=10*1024;
	private InputStream is = null;
	private int linenumber = 0;
	private RetrievalEventProcessor retrievalEventProcessor;
	private RemotableEventStreamDesc currentEventStreamDesc;

	
	InputStreamBackedEventStreamIterator(InputStream is, Timestamp startTime) {
		this.is = is;
		buf = new byte[BUFFER_SIZE];
		readNextBatch();
	}
	
	private byte[] nextLine = null;
	private short year;
	private Constructor<? extends DBRTimeEvent> unmarshallingConstructor;
	
	@Override
	public boolean hasNext() {
		nextLine = readLine();
		if(nextLine == null) return false;
		while(nextLine.length <= 1) {
			logger.debug("Detected PB header. Length of transition line is " + nextLine.length);
			// We have an empty line.
			// Per the protocol, the next line should be a header line.
			try {
				byte[] payloadLine = LineEscaper.unescapeNewLines(this.readLine());
				PayloadInfo info = PayloadInfo.parseFrom(payloadLine);
				RemotableEventStreamDesc desc = new RemotableEventStreamDesc(info.getPvname(), info);
				this.setCurrentEventStreamDesc(desc);
				// The previous line should take care of transitions between year partitions. 
				// We still need to detect PV transitions in the stream and issue events
				// This is done as part of the setCurrentEventStreamDesc itself
			} catch(Exception ex) {
				logger.error("Exception processing PB header info "  + linenumber, ex);
				return false;
			}
			// The line we just read was a header line. 
			// This method needs to leave this iterator in a state where we have the line for the next event
			// So, prepare for the next event by reading another line.
			nextLine = readLine();
			if(nextLine == null) return false;
		}

		return true;
	}
	
	void setCurrentEventStreamDesc(RemotableEventStreamDesc newEventStreamDesc) {
		assert(newEventStreamDesc != null);
		// Check to see if we are processing a new PV.
		// In case this is the first PV, the currentEventStreamDesc will be null.
		boolean processingnewPV = false;
		if(currentEventStreamDesc == null) {
			processingnewPV = true;
		} else if (!newEventStreamDesc.getPvName().equals(currentEventStreamDesc.getPvName())) {
			processingnewPV = true;
			logger.info("Switching to data for PV " + newEventStreamDesc.getPvName());
		} else {
			processingnewPV = false;
			logger.debug("Still in the same PV " + currentEventStreamDesc.getPvName());
		}
		currentEventStreamDesc = newEventStreamDesc;
		unmarshallingConstructor = DBR2PBTypeMapping.getPBClassFor(currentEventStreamDesc.getArchDBRType()).getUnmarshallingFromByteArrayConstructor();
		year = currentEventStreamDesc.getYear();
		if(processingnewPV) {
			// Issue an event
			if(retrievalEventProcessor != null) retrievalEventProcessor.newPVOnStream(currentEventStreamDesc);
		}
	}
	
	@Override
	public Event next() {
		try {
			assert(unmarshallingConstructor != null);
			assert(year != 0);
			return (Event) unmarshallingConstructor.newInstance(year, new ByteArray(nextLine));
		} catch (Exception ex) {
			logger.error("Exception creating event object processing line " + linenumber, ex);
			return null;
		}
	}
	@Override
	public void remove() {
	}

	private byte[] buf = null;
	private long bytesRead = 0;
	private int currentReadPosition = 0;
	byte[] readLine() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		while(true) {
			while(currentReadPosition < bytesRead) {
				byte b = buf[currentReadPosition++];
				if(b == LineEscaper.NEWLINE_CHAR) {
					linenumber++;
					return out.toByteArray();
				} else {
					out.write(b);
				}
			}
			readNextBatch();
			if(bytesRead <= 0) {
				// End of file reached.
				if(out.size() > 0) {
					linenumber++;
					return out.toByteArray();
				} else {
					return null;
				}
			}
		}
	}

	private void readNextBatch() {
		try {
			bytesRead = is.read(buf);
			currentReadPosition = 0;
		}catch(IOException ex) {
			logger.fatal("Exception reading next line ", ex);
			// Probably not kosher but this is needed for the units tests to fail.
			throw new RuntimeException(ex);
		}
	}

	public void setRetrievalEventProcessor(RetrievalEventProcessor retrievalEventProcessor) {
		this.retrievalEventProcessor = retrievalEventProcessor;
	}
}
