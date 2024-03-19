/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.mimeresponses;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.json.simple.JSONValue;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author mshankar
 * Sends the event data as a JSON response.
 * The response is a array of PV elements, each PV has a meta and data section.
 * The data section has timestamp in epoch seconds and the value
 */
public class JSONResponse implements MimeResponse {
	private PrintWriter out;
	boolean needComma = false;
	boolean firstPV = true;
	boolean closePV = false;

	@Override
	public void consumeEvent(Event e) throws Exception {
		DBRTimeEvent evnt = (DBRTimeEvent)e;
		if(!needComma) {
			needComma = true;
		} else {
			out.println(",");
		}
		out.print("{ \"secs\": " + evnt.getEpochSeconds() 
				+ ", \"val\": " + evnt.getSampleValue().toJSONString()
                + ", \"nanos\": " + Integer.toString(evnt.getEventTimeStamp().getNano())
				+ ", \"severity\":" + Integer.toString(evnt.getSeverity())
				+ ", \"status\":" + Integer.toString(evnt.getStatus())
				+ consumeMetadata(evnt)
				+ " }");
	}
	
	private static String consumeMetadata(DBRTimeEvent evnt) { 
		if(evnt.hasFieldValues()) { 
			StringBuilder buf = new StringBuilder();
			buf.append(", \"fields\": { ");
			boolean metaComma = false;
			for(Entry<String, String> keyValue : evnt.getFields().entrySet()) { 
				if(!metaComma) { metaComma = true; } else { buf.append(","); }
				buf.append("\"");
				buf.append(keyValue.getKey());
				buf.append("\": \"");
				buf.append(JSONValue.escape(keyValue.getValue()));
				buf.append("\"");
			}
			buf.append("}");
			return buf.toString();
		} else { 
			return "";
		}
	}

	@Override
	public void setOutputStream(OutputStream os) {
		out = new PrintWriter(os);
		// Perhaps we can use PV name here...
		out.println("[ ");
	}
	
	public void close() {
		if(closePV) {
			out.println(); // Add a new line to make sure we have a sample per line for JSON as well (as best as we can anyways)
			out.println("] }");
		}
		out.println(" ]");
		try { out.flush(); out.close(); } catch(Exception ex) {}
	}

	@Override
    public void processingPV(BasicContext retrievalContext, String pv, Instant start, Instant end, EventStreamDesc streamDesc) {
		if(firstPV) {
			firstPV = false;
		} else {
			out.println("] },");
			needComma = false;
		}
		RemotableEventStreamDesc remoteDesc = (RemotableEventStreamDesc) streamDesc;
		StringWriter buf = new StringWriter();
		buf.append("{ \"meta\": { \"name\": \"").append(pv).append("\" ");
		if(streamDesc != null) {
			Map<String, String> headers = remoteDesc.getHeaders();
			if(!headers.isEmpty()) { 
				for(String fieldName : headers.keySet()) {
					String fieldValue = headers.get(fieldName);
					if(fieldValue != null && !fieldValue.isEmpty()) { 
						buf.append(", \"").append(fieldName).append("\": \"").append(fieldValue).append("\" ");
					}
				}
			}
		}
		buf.append("},\n\"data\": [ ");
		out.println(buf);
		closePV = true;
	}
	
	public void swicthingToStream(EventStream strm) {
		// Not much to do here for now.
	}
}
