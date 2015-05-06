/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.mimeresponses;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.HashMap;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.joda.time.DateTimeZone;

/**
 * @author mshankar
 * Sends the event data as a JSON response tailored to JPlot, which is used in the test page..
 * The response is a array of array of data series: [ series1, series2, ... ]
 * A series can either be raw data or an object with properties. 
 * The raw data format is an array of points: [ [x1, y1], [x2, y2], ... ] E.g. [ [1, 3], [2, 14.01], [3.5, 3.14] ]
 */
public class JPlotResponse implements MimeResponse {
	private PrintWriter out;
	boolean needComma = false;
	boolean firstPV = true;
	boolean closePV = false;
	private DateTimeZone localTimeZone = DateTimeZone.getDefault();

	@Override
	public void consumeEvent(Event e) throws Exception {
		if(!needComma) {
			needComma = true;
		} else {
			out.println(",");
		}
		// Add 3 zeros for millseconds...
		out.print("[" + localTimeZone.convertUTCToLocal(e.getEpochSeconds()*1000) + ", " + e.getSampleValue().toString() + "]");
	}

	@Override
	public void setOutputStream(OutputStream os) {
		out = new PrintWriter(os);
		// We start the array of series here
		out.println("[ ");
	}
	
	public void close() {
		if(closePV) {
			out.println(" ] } ");
		}
		// We close the array of series here
		out.println(" ]");
		try { out.flush(); out.close(); } catch(Exception ex) {}
	}

	@Override
	public void processingPV(String pv, Timestamp start, Timestamp end, EventStreamDesc streamDesc) {
		if(firstPV) {
			firstPV = false;
		} else {
			out.println(" ] } , ");
			needComma = false;
		}
		out.println("{ \"label\": \"" + pv + "\", \"data\": [ ");
		closePV = true;
	}
	
	public void swicthingToStream(EventStream strm) {
		// Not much to do here for now.
	}

	@Override
	public HashMap<String, String> getExtraHeaders() {
		return null;
	}
}
