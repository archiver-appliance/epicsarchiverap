/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.mimeresponses;


import java.io.IOException;
import java.io.OutputStream;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.FieldValue;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo.Builder;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;

/**
 * Mimeresponse for PB over HTTP.
 * @author mshankar
 *
 */
public class PBRAWResponse implements MimeResponse {
	private static final int MERGEDEDUP_BUFFER_EVENTCOUNT = 2;
	private static Logger logger = Logger.getLogger(PBRAWResponse.class.getName());
	private OutputStream os = null;
	boolean firstChunk = true;
	long eventsWritten = 0;
	private Builder headerToBeSentLater = null;
	private String pvName = null;
	private short previouslySentYear = 0;

	@Override
	public void setOutputStream(OutputStream os) {
		this.os = os;
		this.firstChunk = true;
	}

	@Override
	public void processingPV(String pv, Timestamp start, Timestamp end, EventStreamDesc streamDesc) {
		// We don't need this data for the raw response.
		this.pvName = pv;
	}

	public void swicthingToStream(EventStream strm) {
		// The merge dedup consumer has this nasty habit of buffering the first two events and sending them post stream to satisfy various constraints.
		// We have to treat these two events as special as the year that the message header has may not be the one that the event refers to.
		if(eventsWritten > MERGEDEDUP_BUFFER_EVENTCOUNT) {
			writeHeader(strm);
		} else {
			headerToBeSentLater = buildHeader(strm);
			// Write the header anyways to make the chunking code happy. 
			writeHeader(headerToBeSentLater);
		}
	}
	
	
	@Override
	public void consumeEvent(Event e) throws Exception {
		if(eventsWritten <= MERGEDEDUP_BUFFER_EVENTCOUNT) { 
			if(headerToBeSentLater == null) { 
				String msg = "headerToBeSentLater is null when processing pv " + pvName;
				logger.error(msg);
				throw new IOException(msg);
			}
			try { 
				short eventYear = TimeUtils.convertToYearSecondTimestamp(e.getEventTimeStamp()).getYear();
				if(eventYear != previouslySentYear) {
					logger.debug("Writing header as part of event for year " + eventYear);
					headerToBeSentLater.setYear(eventYear);
					writeHeader(headerToBeSentLater);
					previouslySentYear = eventYear;
					logger.debug("Done writing header as part of event for year " + eventYear);
				}
			} catch(Throwable t) { 
				logger.error("Writing header as part of event", t);
			}
		}
		
		ByteArray val = e.getRawForm();
		os.write(val.data, val.off, val.len);
		os.write(LineEscaper.NEWLINE_CHAR);
		eventsWritten++;
	}

	@Override
	public void close() {
		try { os.close(); os = null; } catch(Exception t) {} 
	}
	
	
	private void writeHeader(EventStream strm) { 
		Builder builder = buildHeader(strm);
		writeHeader(builder);
	}
	
	private void writeHeader(Builder builder) { 
		try {
			if(firstChunk) {
				// If this is the first chunk we do not need to add a new line
				firstChunk = false;
			} else {
				os.write(LineEscaper.NEWLINE_CHAR);
			}
			
			byte[] headerBytes = LineEscaper.escapeNewLines(builder.build().toByteArray());
			os.write(headerBytes);
			os.write(LineEscaper.NEWLINE_CHAR);
		} catch(Exception ex) {
			if(ex != null && ex.toString() != null && ex.toString().contains("ClientAbortException")) {
				// We check for ClientAbortException etc this way to avoid including tomcat jars in the build path.
				logger.debug("Exception writing header in the raw response", ex);
			} else { 
				logger.error("Exception writing header in the raw response --> " + ex.toString(), ex);
			}
		}
	}
	
	private static Builder buildHeader(EventStream strm) { 
		RemotableEventStreamDesc desc = ((RemotableOverRaw)strm).getDescription();
		Builder builder = PayloadInfo.newBuilder()
		.setPvname(desc.getPvName())
		.setType(desc.getArchDBRType().getPBPayloadType())
		.setYear(desc.getYear())
		.setElementCount(desc.getElementCount());
		Map<String, String> headers = desc.getHeaders();
		if(!headers.isEmpty()) { 
			LinkedList<FieldValue> fieldValuesList = new LinkedList<FieldValue>();
			for(String fieldName : headers.keySet()) {
				String fieldValue = headers.get(fieldName);
				if(fieldValue != null && !fieldValue.isEmpty()) { 
					fieldValuesList.add(EPICSEvent.FieldValue.newBuilder().setName(fieldName).setVal(fieldValue).build());
				}
			}
			builder.addAllHeaders(fieldValuesList);
		}
		return builder;
	}

	@Override
	public HashMap<String, String> getExtraHeaders() {
		return null;
	}
}
