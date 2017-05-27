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
public class ScannedArchiveChannel extends ArchiveChannel implements Runnable {
	private static final Logger logger = Logger.getLogger(ScannedArchiveChannel.class);
	/** Scan period in seconds */
	final private double scan_period;
	private long scanPeriodMillis;
	/**
	 * Stores the server time when we skip recording values; if the value has not changed when the SCAN comes calling; we record the event.
	 */
	private long serverTimeForStragglingScanValuesMillis = -1;

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
		synchronized(this) { 
			this.serverTimeForStragglingScanValuesMillis = -1;
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

					if(isLessThanOrEqualsScanPeriod(lastDBRTimeEvent, latestDBRTimeEvent)) { 
						// logger.debug("Latest event is less than scan periond; skipping for " + this.getName());
						// We however keep track of the server time when we got a handle event for comparision in the SCAN thread.
						this.serverTimeForStragglingScanValuesMillis = System.currentTimeMillis();
						return true;
					} else { 
						// logger.debug("Latest event is more than scan periond; recording for " + this.getName());
						addValueToBuffer(latestDBRTimeEvent);
					}
					
				} catch (Exception e) {
					logger.error("exception in handleNewValue for pv " + this.getName(), e);
				}
				return true;
			}
			return false;
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 * Called from the SCAN thread...
	 */
	@Override
	public void run() {
		synchronized(this) { 
			if (isEnabled()) {
				try {
					if (latestDBRTimeEvent == null || this.serverTimeForStragglingScanValuesMillis <= 0) {
						// logger.debug("Latest event/straggling time is null " + this.getName());
						return;
					}
					// Is it a new value?
					if (isMatchingTimeStamp(lastDBRTimeEvent, latestDBRTimeEvent)) {
						// logger.debug("Latest event is same as previous ebent " + this.getName());
						return;
					}

					if(isLessThanOrEqualsScanPeriod(this.serverTimeForStragglingScanValuesMillis, System.currentTimeMillis())) { 
						// logger.debug("Latest event is less than scan period; skipping for " + this.getName());
						return;
					} else { 
						// logger.debug("Latest event is more than scan period; recording for " + this.getName());
						addValueToBuffer(latestDBRTimeEvent);
					}

				} catch (Exception e) {
					logger.error("exception in handleNewValue for pv " + this.getName(), e);
				}
				return;
			}
			return;
		}
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
	 * Return true if the second event is within scanPeriodInMillis of the first event.
	 * @param tempEvent1
	 * @param tempEvent2
	 * @return
	 */
	private boolean isLessThanOrEqualsScanPeriod(final DBRTimeEvent tempEvent1, final DBRTimeEvent tempEvent2) { 
		if(tempEvent1 != null && tempEvent2 != null && tempEvent1.getEventTimeStamp() != null && tempEvent2.getEventTimeStamp() != null) { 
			Timestamp time1 = tempEvent1.getEventTimeStamp();
			Timestamp time2 = tempEvent2.getEventTimeStamp();
			// logger.debug("Diff = " + (time2.getTime() - time1.getTime()) + " and scanPeriodMillis " + scanPeriodMillis);
			return (time2.getTime() - time1.getTime()) < this.scanPeriodMillis;
		} else { 
			return false;
		}
	}
	
	/**
	 * Return true if the second event is within scanPeriodInMillis of the first event.
	 * @param tempEvent1
	 * @param tempEvent2
	 * @return
	 */
	private boolean isLessThanOrEqualsScanPeriod(long ts1, long ts2) {
		if(ts1 != -1 && ts2 != -1) { 
			// logger.debug("Diff = " + (ts1 - ts2) + " and scanPeriodMillis " + scanPeriodMillis);
			return (ts2 - ts1) < this.scanPeriodMillis;
		} else { 
			return false;
		}
	}

}
