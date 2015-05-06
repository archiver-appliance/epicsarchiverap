/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.membuf;

import java.util.ArrayList;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

/**
 * Implements an event stream on top of an arraylist
 * @author mshankar
 *
 */
@SuppressWarnings("serial")
public class ArrayListEventStream extends ArrayList<Event> implements EventStream, RemotableOverRaw {
	private RemotableEventStreamDesc desc;
	
	public ArrayListEventStream(int initialSize, RemotableEventStreamDesc desc) {
		super(initialSize);
		this.desc = desc;
	}

	@Override
	public void close() {
		// Nothing to do...
	}

	@Override
	public RemotableEventStreamDesc getDescription() {
		return desc;
	}
	
	public short getYear()
	{
		return desc.getYear();
	}

	public void setYear(short year)
	{
		desc.setYear(year);
	}
}
