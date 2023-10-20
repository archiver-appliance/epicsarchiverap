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
import org.epics.archiverappliance.common.BasicContext;

/**
 * Test response of action script+XML with large data sets
 * For now this is only for one PV.
 * 
 * @author mshankar
 *
 */
public class FlxXMLResponse implements MimeResponse {
	private PrintWriter out;
	boolean needComma = false;
	boolean firstPV = true;
	boolean closePV = false;

	@Override
	public void consumeEvent(Event e) throws Exception {
		out.print("<result><time>" + e.getEpochSeconds()*1000 + "</time><value>" + e.getSampleValue().toString() + "</value></result>");
	}

	@Override
	public void setOutputStream(OutputStream os) {
		out = new PrintWriter(os);
		// Perhaps we can use PV name here...
		out.println("<data>");
	}
	
	public void close() {
		out.println("</data>");
		try { out.flush(); out.close(); } catch(Exception ex) {}
	}

	@Override
	public void processingPV(BasicContext retrievalContext, String pv, Timestamp start, Timestamp end, EventStreamDesc streamDesc) {
	}
	
	public void swicthingToStream(EventStream strm) {
	}
}
