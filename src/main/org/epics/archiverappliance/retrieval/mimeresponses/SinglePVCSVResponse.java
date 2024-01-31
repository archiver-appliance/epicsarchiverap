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

import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.HashMap;

/**
 * Implementation of a CSV response for a single PV
 * Currently, we are restricting the CSV response to only one PV. 
 * 
 * @author mshankar
 *
 */
public class SinglePVCSVResponse implements MimeResponse {
	private PrintWriter out;

	@Override
	public void consumeEvent(Event e) throws Exception {
		out.println( e.getEpochSeconds() 
				+ "," + e.getSampleValue().toString()
				+ "," + (((DBRTimeEvent)e).getSeverity())
				+ "," + (((DBRTimeEvent)e).getStatus())
                        + "," + (((DBRTimeEvent) e).getEventTimeStamp().getNano())
				);
	}

	@Override
	public void setOutputStream(OutputStream os) {
		out = new PrintWriter(os);
	}
	
	public void close() {
		out.println();
		try { out.flush(); out.close(); } catch(Exception ex) {}
	}

	@Override
    public void processingPV(BasicContext retrievalContext, String pv, Instant start, Instant end, EventStreamDesc streamDesc) {
		// Not much to do here for now.
	}
	
	public void swicthingToStream(EventStream strm) {
		// Not much to do here for now.
	}
}