/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
/**
 * Store all samples in a buffer
 * @author Murali Shankar
 *
 */
public class MemBufWriter implements Writer {
	private ArrayListEventStream buf;
	
	public MemBufWriter(String pvName, ArchDBRTypes dbrType) { 
		buf = new ArrayListEventStream(1024, new RemotableEventStreamDesc(dbrType, pvName, TimeUtils.getCurrentYear()));
	}

	@Before
	public void setUp() throws Exception {

	}

	@After
	public void tearDown() throws Exception {

	}

	@Test
	public void testNothing() {

	}

	@Override
    public int appendData(BasicContext context, String arg0, EventStream arg1) throws IOException {
        int eventsAppended = 0;
		for(Event e : arg1) {
			buf.add(e);
            eventsAppended++;
		}
        return eventsAppended;
	}

	@Override
	public Event getLastKnownEvent(BasicContext context, String pvName)
			throws IOException {
		return null;
	}

	public EventStream getCollectedSamples() throws IOException {
		return buf;
	}

}
