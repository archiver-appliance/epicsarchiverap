/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.etl;

import java.io.IOException;
import java.util.HashMap;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;

/**
 * A POJO that encapsulates all the information needed about a stream from an ETL source
 * @author mshankar
 *
 */
public class ETLInfo {
	private String pvName;
	private String key;
	private Event firstEvent;
	private PartitionGranularity granularity;
	private ETLStreamCreator strmCreator;
	private ArchDBRTypes type;
	private long size = -1;
	private HashMap<String, String> otherInfo = new HashMap<String, String>();
	
	public ETLInfo(String pvName, ArchDBRTypes type, String key, PartitionGranularity granularity, ETLStreamCreator strmCreator, Event firstEvent, long size) {
		this.pvName = pvName;
		this.type = type;
		this.key = key;
		this.granularity = granularity;
		this.strmCreator = strmCreator;
		this.firstEvent = firstEvent;
		this.size = size;
	}
	public ArchDBRTypes getType() {
		return type;
	}
	public String getPvName() {
		return pvName;
	}
	public String getKey() {
		return key;
	}
	public PartitionGranularity getGranularity() {
		return granularity;
	}
	public EventStream getEv() throws IOException {
		return strmCreator.getStream();
	}
	public Event getFirstEvent() {
		return firstEvent;
	}
	
	
	public void addOtherInfo(String name, String value) { 
		otherInfo.put(name, value);
	}
	
	public String getOtherInfo(String name){ 
		return otherInfo.get(name);
	}
	public long getSize() {
		return size;
	}
	
	public ETLStreamCreator getStrmCreator() {
		return strmCreator;
	}
	public void setStrmCreator(ETLStreamCreator strmCreator) {
		this.strmCreator = strmCreator;
	}
}
