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
 * An ArchiveChannel that stores each incoming value that differs from the
 * previous sample by some 'delta'.
 * 
 * @author Kay Kasemir
 */
@SuppressWarnings("nls")
public class DeltaArchiveChannel extends ArchiveChannel {
	private static final Logger logger = Logger.getLogger(DeltaArchiveChannel.class);

	/** 'Delta' for value change */
	final private double delta;

	/** Estimated period of change in seconds */
	final private double period_estimate;

	/**
	 * @param name
	 *            Name of the channel (PV)
	 * @param enablement
	 *            How channel affects its groups
	 * @param buffer_capacity
	 *            Size of sample buffer
	 * @param last_archived_value
	 *            Last value from storage, or <code>null</code>.
	 * @param period_estimate
	 *            Estimated change period [seconds]
	 * @param delta
	 *            Value changes &ge; this value will be stored
	 * @throws Exception
	 *             On error in PV setup
	 */
	public DeltaArchiveChannel(final String name, final Writer writer,
			final Enablement enablement, final int buffer_capacity,
			final Timestamp last_timeestamp, final double period_estimate,
			final double delta, final ConfigService configservice,
			final ArchDBRTypes archdbrtype, final String controlPVname,
			final int commandThreadID, final boolean usePVAccess) throws Exception {
		super(name, writer, enablement, buffer_capacity, last_timeestamp,
				configservice, archdbrtype, controlPVname, commandThreadID, usePVAccess);
		this.delta = delta;
		this.period_estimate = period_estimate;
	}

	@Override
	public String getMechanism() {
		return "on delta [" + PeriodFormat.formatSeconds(period_estimate)
				+ ", " + delta + "]";
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
		} catch (Exception e1) {
			//
			logger.error("Exception handing new value", e1);
		}
		if (isEnabled() && isBeyondDelta(timeevent)) {
			// Activator.getLogger().log(Level.FINE,
			// "Wrote sample for {0}: {1}", new Object[] { getName(), value });
			try {
				addValueToBuffer(timeevent);
			} catch (Exception e) {
				//
				logger.error("exception in handleNewValue", e);
			}
			return true;
		}
		return false;
	}

	/**
	 * @param value
	 *            Value to test
	 * @return <code>true</code> if this value is beyond 'delta' from the last
	 *         value
	 */
	private boolean isBeyondDelta(final DBRTimeEvent timeevent) {
		final double number = ValueUtil.getDouble(timeevent.getSampleValue());
		// Archive NaN, Inf'ty
		if (Double.isNaN(number))
			return true;
		double previous;
		synchronized (this) {
			// Anything to compare against?
			if (lastDBRTimeEvent == null)
				return true;
			previous = ValueUtil.getDouble(timeevent.getSampleValue());
		}
		return Math.abs(previous - number) >= delta;
	}
}
