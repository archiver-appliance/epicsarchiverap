/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.time.Instant;

/**
 * An ArchiveChannel that stores each incoming value.
 * 
 * @author Kay Kasemir
 * @version Initial version:CSS
 * @version 4-Jun-2012, Luofeng Li:added codes to support for the new archiver
 */
@SuppressWarnings("nls")
public class MonitoredArchiveChannel extends ArchiveChannel {
	private static final Logger logger = LogManager.getLogger(MonitoredArchiveChannel.class);
	/** Estimated period of change in seconds */
	final private double period_estimate;

       /** @see ArchiveChannel#ArchiveChannel
	 * @param name pv's name
	 * @param writer the writer for this pv
	 * @param buffer_capacity the sample buffer's capacity for this pv
	 * @param last_archived_timestamp the last time stamp when this pv was archived
	 * @param period_estimate  &emsp;
	 * @param configservice the configservice of new archiver
	 * @param archdbrtype the archiving dbr type
	 * @param controlPVname the pv's name who control this pv to start archiving or stop archiving
	 * @param commandThreadID - this is the index into the array of JCA command threads that processes this context.
	 * @param usePVAccess - Should we use PVAccess to connect to this PV.
	 * @throws Exception error when creating archive channel for this pv
	 */
	public MonitoredArchiveChannel(final String name, final Writer writer,
			final int buffer_capacity,
                                   final Instant last_archived_timestamp,
			final double period_estimate, final ConfigService configservice,
			final ArchDBRTypes archdbrtype, final String controlPVname,
			final int commandThreadID, final boolean usePVAccess) throws Exception {
		super(name, writer, buffer_capacity,
				last_archived_timestamp, configservice, archdbrtype,
				controlPVname, commandThreadID, usePVAccess);
		this.period_estimate = period_estimate;
		this.pvMetrics.setSamplingPeriod(period_estimate);
		this.pvMetrics.setMonitor(true);
	}

	@Override
	public String getMechanism() {
		return "on change [" + PeriodFormat.formatSeconds(period_estimate)
				+ "]";
	}

	/** Attempt to add each new value to the buffer. */
	@Override
	protected boolean handleNewValue(final DBRTimeEvent timeevent) {
		try {
			if (super.handleNewValue(timeevent)) {

				return true;
			}
		} catch (Exception e) {
			logger.error("exception in handleNewValue for pv" + this.getName(), e);
		}
		try {
			addValueToBuffer(timeevent);
		} catch (Exception e) {
			logger.error("exception in handleNewValue for pv " + this.getName(), e);
		}
		return true;
	}
}
