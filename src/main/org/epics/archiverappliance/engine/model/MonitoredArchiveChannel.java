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
 * An ArchiveChannel that stores each incoming value.
 * 
 * @author Kay Kasemir
 * @version Initial version:CSS
 * @version 4-Jun-2012, Luofeng Li:added codes to support for the new archiver
 */
@SuppressWarnings("nls")
public class MonitoredArchiveChannel extends ArchiveChannel {
	private static final Logger logger = Logger.getLogger(MonitoredArchiveChannel.class);
	/** Estimated period of change in seconds */
	final private double period_estimate;

	/** @see ArchiveChannel#ArchiveChannel(String, Writer, Enablement,int,Timestamp,double,ConfigService,ArchDBRTypes,String,boolean) */
	public MonitoredArchiveChannel(final String name, final Writer writer,
			final Enablement enablement, final int buffer_capacity,
			final Timestamp last_archived_timestamp,
			final double period_estimate, final ConfigService configservice,
			final ArchDBRTypes archdbrtype, final String controlPVname,
			final int commandThreadID, final boolean usePVAccess) throws Exception {
		super(name, writer, enablement, buffer_capacity,
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
		if (isEnabled()) {
			try {
				addValueToBuffer(timeevent);
			} catch (Exception e) {
				logger.error("exception in handleNewValue for pv " + this.getName(), e);
			}
			return true;
		}
		return false;
	}
}
