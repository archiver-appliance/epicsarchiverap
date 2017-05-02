/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import java.sql.Timestamp;

/**
 * Simple time span class with start and end times.
 * @author mshankar
 *
 */
public class TimeSpan {
	Timestamp startTime;
	Timestamp endTime;
	public Timestamp getStartTime() {
		return startTime;
	}
	public Timestamp getEndTime() {
		return endTime;
	}
	public TimeSpan(Timestamp startTime, Timestamp endTime) {
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public TimeSpan(long startTimeEpochSeconds, long endTimeEpochSeconds) {
		this.startTime = TimeUtils.convertFromEpochSeconds(startTimeEpochSeconds, 0);
		this.endTime = TimeUtils.convertFromEpochSeconds(endTimeEpochSeconds, 0);
	}
	
	/**
	 * True if this timespan completely contains the other timestamp.
	 * @param other TimeSpan
	 * @return boolean True or False
	 */
	public boolean contains(TimeSpan other) { 
		return (this.startTime.before(other.startTime) || this.startTime.equals(other.startTime)) &&
				(this.endTime.after(other.endTime) || this.endTime.equals(other.endTime));
	}
}
