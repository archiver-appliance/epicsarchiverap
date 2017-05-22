/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.model;

import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;

/**
 * An ArchiveChannel that stores value in a periodic scan.
 * 
 * @author Kay Kasemir
 * @version Initial version:CSS
 * @version 4-Jun-2012, Luofeng Li:added codes to support for the new archiver
 */
@SuppressWarnings("nls")
public class ScannedArchiveChannel extends ArchiveChannel {
	private static final Logger logger = Logger.getLogger(ScannedArchiveChannel.class);
	/** Scan period in seconds */
	final private double scan_period;
	private long scanPeriodMillis;

	/** @see ArchiveChannel#ArchiveChannel
	 * @param name pv's name
	 * @param writer the writer for this pv
	 * @param enablement  start or stop archiving this pv when channel is created
	 * @param buffer_capacity the sample buffer's capacity for this pv
	 * @param last_timeestamp the last time stamp when this pv was archived
	 * @param scan_period  &emsp;
	 * @param configservice the configservice of new archiver
	 * @param archdbrtype the archiving dbr type
	 * @param controlPVname the pv's name who control this pv to start archiving or stop archiving
	 * @param commandThreadID - this is the index into the array of JCA command threads that processes this context.
	 * @param usePVAccess - Should we use PVAccess to connect to this PV.
	 * @throws Exception error when creating archive channel for this pv
	 */
	public ScannedArchiveChannel(final String name, final Writer writer,
			Enablement enablement, final int buffer_capacity,
			final Timestamp last_timeestamp, final double scan_period,
			final ConfigService configservice, final ArchDBRTypes archdbrtype,
			final String controlPVname, final int commandThreadID, final boolean usePVAccess)
			throws Exception {
		super(name, writer, enablement, buffer_capacity, last_timeestamp,
				configservice, archdbrtype, controlPVname, commandThreadID, usePVAccess);
		this.scan_period = scan_period;
		this.pvMetrics.setSamplingPeriod(scan_period);
		// this.max_repeats = max_repeats;
		this.pvMetrics.setMonitor(false);
		this.scanPeriodMillis = (long) scan_period * 1000;
	}

	/** @return Scan period in seconds */
	final public double getPeriod() {
		return scan_period;
	}

	@Override
	public String getMechanism() {
		return PeriodFormat.formatSeconds(scan_period);
	}

	// Just for debugging...
	@Override
	protected boolean handleNewValue(final DBRTimeEvent timeevent) throws Exception {
		try {
			if (super.handleNewValue(timeevent)) {

				return true;
			}
		} catch (Exception e) {
			logger.error("exception in handleNewValue for pv" + this.getName(), e);
		}
		
		if (isEnabled()) {
			try {
				if (latestDBRTimeEvent == null) {
					return true;
				}
				// Is it a new value?
				if (isMatchingTimeStamp(lastDBRTimeEvent, latestDBRTimeEvent)) {
					return true;
				}

				if(isMoreThanOrEqualsScanPeriod(lastDBRTimeEvent, latestDBRTimeEvent)) { 
					// logger.debug("Latest event is more than scan periond; recording...");
					addValueToBuffer(latestDBRTimeEvent);
					return true;
				}
				
			} catch (Exception e) {
				logger.error("exception in handleNewValue for pv " + this.getName(), e);
			}
			return true;
		}
		return false;
	}

	/**
	 * check whether the two timeevent have the same time stamp
	 * @param tempEvent1 time event 1
	 * @param tempEvent2  time event 2
	 * @return true if they have the same time stamps. Other wise ,false
	 */
	private boolean isMatchingTimeStamp(final DBRTimeEvent tempEvent1, final DBRTimeEvent tempEvent2) {
		if(tempEvent1 != null && tempEvent2 != null && tempEvent1.getEventTimeStamp() != null && tempEvent2.getEventTimeStamp() != null) { 
			Timestamp time1 = tempEvent1.getEventTimeStamp();
			Timestamp time2 = tempEvent2.getEventTimeStamp();
			return time1.equals(time2);
		} else { 
			return false;
		}
	}
	
	/**
	 * Return true if the second event is scanPeriodInMillis more than the first event.
	 * @param tempEvent1
	 * @param tempEvent2
	 * @return
	 */
	private boolean isMoreThanOrEqualsScanPeriod(final DBRTimeEvent tempEvent1, final DBRTimeEvent tempEvent2) { 
		if(tempEvent1 != null && tempEvent2 != null && tempEvent1.getEventTimeStamp() != null && tempEvent2.getEventTimeStamp() != null) { 
			Timestamp time1 = tempEvent1.getEventTimeStamp();
			Timestamp time2 = tempEvent2.getEventTimeStamp();
			// logger.debug("Diff = " + (time2.getTime() - time1.getTime()) + " and scanPeriodMillis " + scanPeriodMillis);
			return (time2.getTime() - time1.getTime()) >= this.scanPeriodMillis;
		} else { 
			return false;
		}
	}

}
