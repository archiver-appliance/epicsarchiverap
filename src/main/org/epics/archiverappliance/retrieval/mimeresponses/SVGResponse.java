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
 * @author mshankar
 * Extremely simplistic SVG mime response.
 */
public class SVGResponse implements MimeResponse {
	
	private PrintWriter out;
	private long viewboxX;
	private long viewboxW;
	private boolean writtenHeader = false;

	@Override
	public void consumeEvent(Event e) throws Exception {
		out.print("L " + e.getEpochSeconds() + " " + e.getSampleValue().toString() + " ");
	}

	@Override
	public void setOutputStream(OutputStream os) {
		out = new PrintWriter(os);
	}
	
	public void close() {
		out.println(" \"/>");
		out.println("</svg>");
		try { out.flush(); out.close(); } catch(Exception ex) {}
	}

	@Override
	public void processingPV(BasicContext retrievalContext, String pv, Timestamp start, Timestamp end, EventStreamDesc streamDesc) {
		viewboxX = start.getTime()/1000;
		viewboxW = (end.getTime()/1000) - viewboxX;
		double minY = -1.0;
		if(!writtenHeader) {
			writtenHeader = true;
			out.println("<svg xmlns:svg=\"http://www.w3.org/2000/svg\" xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"" 
					+ viewboxX + minY 
					+ viewboxW + " 4.0\" " 
					+ "width=\"1000px\" height=\"500px\">");
			out.print("<path style=\"fill:none;stroke:red;stroke-width:1.0\" d=\"m " + viewboxX + " " + minY);
		}
	}

	@Override
	public void swicthingToStream(EventStream strm) {
		// Not much to do here for now.
	}
}
