/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval;

import org.epics.archiverappliance.StoragePlugin;
import org.epics.archiverappliance.common.TimeSpan;

import java.time.Instant;

/**
 * Represents a data source for data for a PV
 * @author mshankar
 *
 */
public class DataSourceforPV implements Comparable<DataSourceforPV> {
	private String pvName;
	private StoragePlugin storagePlugin;
	private int lifetimeId;
    private Instant dataStartTime;
    private Instant dataEndTime;
	
	public DataSourceforPV(String pvName, StoragePlugin storagePlugin, int lifetimeid,
                           Instant dataStartTime, Instant dataEndTime) {
		super();
		this.pvName = pvName;
		this.storagePlugin = storagePlugin;
		this.lifetimeId = lifetimeid;
		this.dataStartTime = dataStartTime;
		this.dataEndTime = dataEndTime;
	}
	
	public String getPvName() {
		return pvName;
	}

    public Instant getDataStartTime() {
		return dataStartTime;
	}

    public Instant getDataEndTime() {
		return dataEndTime;
	}
	public StoragePlugin getStoragePlugin() {
		return storagePlugin;
	}

	public int getLifetimeId() {
		return lifetimeId;
	}

	@Override
	public boolean equals(Object other) {
		return (this.lifetimeId == ((DataSourceforPV) other).lifetimeId);
	}

	@Override
	public int hashCode() {
		return Integer.valueOf(this.lifetimeId).hashCode();
	}

	@Override
	public int compareTo(DataSourceforPV other) {
		// Lower lifetimeid means later in the request queue.
		return other.lifetimeId - this.lifetimeId;
	}
	
	/**
	 * Has the data source resolution specified the times for which we are fetching the data?
	 * Note this is an optional optimization and is most often used when getting data from the ChannelArchiver.
	 * Most other times, we let this default to the start time and end time of the data retrieval request.
	 * @return boolean True or False
	 */
	public boolean isOverridingStartAndEndTimes() { 
		return this.dataStartTime != null && this.dataEndTime != null;
	}
	
	public TimeSpan getRequestTimeSpan() { 
		return new TimeSpan(this.dataStartTime, this.dataEndTime);
	}
}
