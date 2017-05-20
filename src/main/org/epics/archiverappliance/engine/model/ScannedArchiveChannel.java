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
	protected boolean handleNewValue(final DBRTimeEvent timeevent)
			throws Exception {
		final boolean written = super.handleNewValue(timeevent);
		this.getPVMetrics().incrementScanEventCount();
		return written;
	}

	/**
	 * Invoked by periodic scanner. Try to add the most recent value to the
	 * archive. Skip repeated values, unless we exceed the max. repeat count.
	 */
	@Override
	final public void run() {

		if (!isEnabled())
			return;
		long start = System.currentTimeMillis();
		try {
			synchronized (this) { // Have anything?
				if (latestDBRTimeEvent == null) {
					return;
				}
				// Is it a new value?
				if (isMatchingTimeStamp(lastDBRTimeEvent, latestDBRTimeEvent)) {
					logger.debug("Skipping events that have the same timestamp");
					return;
				}
			}
			addValueToBuffer(latestDBRTimeEvent);
		} catch (Exception e) {
			logger.error("exception duing run for pv " + this.getName(), e);
		} finally { 
			try { 
				long end = System.currentTimeMillis();
				this.getPVMetrics().setScanProcessingTime(start, end, scan_period);
			} catch(Exception ex){ 
				logger.error("Exception updating scan times for " + this.getName(), ex);
			}
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

}
