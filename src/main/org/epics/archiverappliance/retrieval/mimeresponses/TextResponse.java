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
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.HashMap;

/**
 * A human readable text response; mostly to be used for debugging etc.
 * @author mshankar
 *
 */
public class TextResponse implements MimeResponse, ExceptionCommunicator {
	private PrintWriter out;
	boolean firstPV = true;

	@Override
	public void consumeEvent(Event e) throws Exception {
		out.println(TimeUtils.convertToHumanReadableString(TimeUtils.convertFromEpochSeconds(e.getEpochSeconds(), 0)) 
				+ "\t" + e.getSampleValue().toString()
				+ "\t" + (((DBRTimeEvent)e).getSeverity())
				+ "\t" + (((DBRTimeEvent)e).getStatus())
                        + "\t" + (((DBRTimeEvent) e).getEventTimeStamp().getNano())
				);
	}

	@Override
	public void setOutputStream(OutputStream os) {
		out = new PrintWriter(os);
	}
	
	public void close() {
		try { out.flush(); out.close(); } catch(Exception ex) {}
	}

	@Override
    public void processingPV(BasicContext retrievalContext, String pv, Instant start, Instant end, EventStreamDesc streamDesc) {
		out.println("Beginning data for " + pv);
	}
	
	public void swicthingToStream(EventStream strm) {
		out.println("Data from stream " + strm.getDescription().getSource());
	}

	@Override
	public void comminucateException(Throwable t) {
		out.println(t.getMessage());
		t.printStackTrace(out);
	}
}
