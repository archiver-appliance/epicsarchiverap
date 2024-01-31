/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.mimeresponses;

import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.retrieval.EventConsumer;

import java.io.OutputStream;
import java.time.Instant;
import java.util.HashMap;

/**
 * A set of events for classes that convert event streams to mime specific responses.
 * setOutputStream signals the start of the whole package
 * processingPV signals the start of a new PV's data in the package
 * swicthingToStream signals that we have a new event stream in the current PV; so stream specific headers could be added here.
 * close signifies the end of the whole package.
 * 
 * Note we get both one processingPV and potentially many swicthingToStream for each PV in the package. 
 * @author mshankar
 *
 */
public interface MimeResponse extends EventConsumer {
	public void setOutputStream(OutputStream os);
	/**
	 * Called when we swich to a new PV. 
	 * @param pv The name of PV 
     * @param start Instant
     * @param end Instant
	 * @param streamDesc  Could be null if we have no data in first store we hit.
	 */
    public void processingPV(BasicContext retrievalContext, String pv, Instant start, Instant end, EventStreamDesc streamDesc);
	public void swicthingToStream(EventStream strm);
	public void close();
	
	
	public static final String CONTENT_TYPE = "Content-Type";
	/**
	 * Headers that should be proxied across in a clustered environment should be added to this list.
	 */
	public static final String[] PROXIED_HEADERS = {CONTENT_TYPE};
}
