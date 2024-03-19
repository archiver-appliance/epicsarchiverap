/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

/**
 * An event stream should also support this additional information about the stream.
 * In addition, event streams that are to be sent across the wire should implement the RemotableOverRaw extension.
 * The source is a string used to describe the source of the stream and is meant for logging/debugging purposes.
 * @author mshankar
 * @see RemotableOverRaw
 */
public class EventStreamDesc {

	protected ArchDBRTypes archDBRType;
	protected String pvName;
	protected String source;

	public EventStreamDesc(ArchDBRTypes archDBRType, String pvName) {
		super();
		this.archDBRType = archDBRType;
		this.pvName = pvName;
	}
	
	public EventStreamDesc(EventStreamDesc other) { 
		this.archDBRType = other.archDBRType;
		this.pvName = other.pvName;
		this.source = other.source;
	}

	public ArchDBRTypes getArchDBRType() {
		return archDBRType;
	}

	public void setArchDBRType(ArchDBRTypes archDBRType) {
		this.archDBRType = archDBRType;
	}

	public String getPvName() {
		return pvName;
	}

	public void setPvName(String pvName) {
		this.pvName = pvName;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}
}