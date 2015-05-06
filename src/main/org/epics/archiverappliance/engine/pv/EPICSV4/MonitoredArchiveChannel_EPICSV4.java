/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.pv.EPICSV4;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.model.Enablement;
import org.epics.archiverappliance.engine.model.PeriodFormat;

/**
 * An ArchiveChannel that stores each incoming value.
 * 
 * @author Kay Kasemir
 */
@SuppressWarnings("nls")
public class MonitoredArchiveChannel_EPICSV4 extends ArchiveChannel_EPICSV4 {
	private static final Logger logger = Logger.getLogger(MonitoredArchiveChannel_EPICSV4.class);
	/** Estimated period of change in seconds */
	final private double period_estimate;

	/** @see ArchiveChannel#ArchiveChannel(String, int, IValue) */
	public MonitoredArchiveChannel_EPICSV4(final String name,
			final Enablement enablement, final int buffer_capacity,
			final DBRTimeEvent last_archived_value,
			final double period_estimate, final ConfigService configservice,
			final ArchDBRTypes archdbrtype) throws Exception {
		super(name, enablement, buffer_capacity, last_archived_value,
				configservice, archdbrtype);
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
				// Activator.getLogger().log(Level.FINE,
				// "Wrote first sample for {0}: {1}", new Object[] { getName(),
				// value });
				return true;
			}
		} catch (Exception e) {
			logger.error("exception in handleNewValue", e);
		}
		if (isEnabled()) {
			// Activator.getLogger().log(Level.FINE,
			// "Wrote sample for {0}: {1}", new Object[] { getName(), value });
			try {
				addValueToBuffer(timeevent);
			} catch (Exception e) {
				logger.error("exception in handleNewValue", e);
			}
			return true;
		}
		return false;
	}
}
