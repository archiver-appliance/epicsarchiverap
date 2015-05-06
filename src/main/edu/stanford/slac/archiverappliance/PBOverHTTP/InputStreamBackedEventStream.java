/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PBOverHTTP;


import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.EmptyEventIterator;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;
import org.epics.archiverappliance.retrieval.client.RetrievalEventProcessor;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;

/**
 * An EventStream that is backed by an arbitrary input stream.
 * @author mshankar
 *
 */
public class InputStreamBackedEventStream implements EventStream, RemotableOverRaw {
	private static Logger logger = Logger.getLogger(InputStreamBackedEventStream.class.getName());
	// We expect the PB headers to fit within this buffer size.
	private static int MAX_PB_HEADER_LINE_SIZE = 10*1024;
	private RemotableEventStreamDesc descFromFirstLine;
	private RetrievalEventProcessor retrievalEventProcessor;
	InputStream is = null;
	Timestamp startTime;
	private InputStreamBackedEventStreamIterator theIterator;

	public InputStreamBackedEventStream(InputStream is, Timestamp startTime) throws IOException {
		this.is = is;
		this.startTime = startTime;
		assert(is.markSupported());
		is.mark(MAX_PB_HEADER_LINE_SIZE);
		try {
			// Read the PB descriptor from the first line but put the stream back to where it was. 
			InputStreamBackedEventStreamIterator ret = new InputStreamBackedEventStreamIterator(this.is, startTime);
			byte[] line = ret.readLine();
			if(line == null) return;
			byte[] firstLine = LineEscaper.unescapeNewLines(line);
			PayloadInfo info = PayloadInfo.parseFrom(firstLine);
			descFromFirstLine = new RemotableEventStreamDesc(info.getPvname(), info);
			ret.setCurrentEventStreamDesc(descFromFirstLine);
			logger.debug("Done reading desc from first line");
		} catch(Exception ex) {
			logger.error("Exception parsing header", ex);
			throw new IOException(ex);
		} finally {
			try { is.reset(); } catch (Throwable t) { logger.error("Exception resetting mark in input stream", t); } 
		}
	}
	
	public InputStreamBackedEventStream(InputStream is, Timestamp startTime, RetrievalEventProcessor retrievalEventProcessor) throws IOException {
		this(is, startTime);
		this.retrievalEventProcessor = retrievalEventProcessor;
	}
	

	@Override
	public Iterator<Event> iterator() {
		// The input stream has state. We could use a TeeInputStream to support multiple iterators but for now, we insist on one iterator per event stream.
		assert(theIterator == null);
		// Return an empty iterator...
		if(descFromFirstLine == null) return new EmptyEventIterator();
		theIterator = new InputStreamBackedEventStreamIterator(this.is, startTime);
		if(retrievalEventProcessor != null) theIterator.setRetrievalEventProcessor(retrievalEventProcessor);
		try {
			byte[] firstLine = LineEscaper.unescapeNewLines(theIterator.readLine());
			PayloadInfo info = PayloadInfo.parseFrom(firstLine);
			descFromFirstLine = new RemotableEventStreamDesc(info.getPvname(), info);
			theIterator.setCurrentEventStreamDesc(descFromFirstLine);
		} catch(Exception ex) {
			logger.error("Exception parsing header", ex);
		}
		return theIterator;
	}

	@Override
	public void close() {
		try { is.close(); } catch (Throwable t) {}
	}

	@Override
	public RemotableEventStreamDesc getDescription() {
		return descFromFirstLine;
	}

}
