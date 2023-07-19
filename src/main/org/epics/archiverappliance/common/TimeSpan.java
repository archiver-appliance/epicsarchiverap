/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.common;

import java.time.Instant;

/**
 * Simple time span class with start and end times.
 * @author mshankar
 *
 */
public class TimeSpan {
    Instant startTime;
    Instant endTime;

    public TimeSpan(Instant startTime, Instant endTime) {
		this.startTime = startTime;
		this.endTime = endTime;
	}

    public Instant getStartTime() {
		return startTime;
	}

    public Instant getEndTime() {
		return endTime;
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
        return (this.startTime.isBefore(other.startTime) || this.startTime.equals(other.startTime)) &&
                (this.endTime.isAfter(other.endTime) || this.endTime.equals(other.endTime));
	}

	@Override
	public String toString() {
		return this.startTime + " - " + this.endTime;
	}
}
