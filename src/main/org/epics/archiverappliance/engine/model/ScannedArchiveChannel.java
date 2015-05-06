/*******************************************************************************
 * Copyright (c) 2010 Oak Ridge National Laboratory.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 ******************************************************************************/
package org.epics.archiverappliance.engine.model;

import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Writer;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;

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

	/** @see ArchiveChannel#ArchiveChannel(String, Writer, Enablement,int,Timestamp,double,ConfigService,ArchDBRTypes,String,boolean)*/
	public ScannedArchiveChannel(final String name, final Writer writer,
			Enablement enablement, final int buffer_capacity,
			final Timestamp last_timeestamp, final double scan_period,
			final ConfigService configservice, final ArchDBRTypes archdbrtype,
			final String controlPVname, final boolean isMetaField, final int commandThreadID)
			throws Exception {
		super(name, writer, enablement, buffer_capacity, last_timeestamp,
				configservice, archdbrtype, controlPVname, isMetaField, commandThreadID);
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
		// if (! written)
		// Activator.getLogger().log(Level.FINE, "{0} cached {1}", new Object[]
		// { getName(), value });
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
		try {
			synchronized (this) { // Have anything?
				if (latestDBRTimeEvent == null) {

					return;
				}
				// Is it a new value?

				if (isMatchingValue(lastDBRTimeEvent, latestDBRTimeEvent)) {

					if (isMatchingTimeStamp(lastDBRTimeEvent,
							latestDBRTimeEvent)) {
						// System.out.println("same value");
						return;
					}

				}

				// New value, or exceeded repeats
				// repeats = 0;
			}
			// unlocked, should have 'value'

			addValueToBuffer(latestDBRTimeEvent);
		} catch (Exception e) {
			logger.error("exception duing run", e);
		}
	}

	/**
	 * Check if values match in status, severity, and value. Time is ignored.
	 * 
	 * @param val1
	 *            One value
	 * @param val2
	 *            Other value
	 * @return <code>true</code> if they match 
	 */
	private boolean isMatchingValue(final DBRTimeEvent tempEvent1,
			final DBRTimeEvent tempEvent2) {
		// Compare data type and value
		if (tempEvent1 == null)
			return false;
		SampleValue val1 = tempEvent1.getSampleValue();
		SampleValue val2 = tempEvent2.getSampleValue();
		if (val1 instanceof VectorValue) {
			if (!(val2 instanceof VectorValue))
				return false;

			VectorValue<?> vv1 = (VectorValue<?>) val1;
			VectorValue<?> vv2 = (VectorValue<?>) val2;
			final Object values1[] = vv1.getValues().toArray();
			final Object values2[] = vv2.getValues().toArray();
			if (!Arrays.equals(values1, values2))
				return false;

		}

		else if (val1 instanceof ScalarValue) {
			if (!(val2 instanceof ScalarValue))
				return false;

			ScalarValue<?> vv1 = (ScalarValue<?>) val1;
			ScalarValue<?> vv2 = (ScalarValue<?>) val2;
			Double dou1 = vv1.getValue().doubleValue();
			Double dou2 = vv2.getValue().doubleValue();
			if ((dou1 - dou2) != 0)
				return false;

		}
		// ScalarStringSampleValue VectorStringSampleValue
		else if (val1 instanceof ScalarStringSampleValue) {
			if (!(val2 instanceof ScalarStringSampleValue))
				return false;

			ScalarStringSampleValue vv1 = (ScalarStringSampleValue) val1;
			ScalarStringSampleValue vv2 = (ScalarStringSampleValue) val2;
			String str1 = vv1.toString();
			String str2 = vv2.toString();
			if (str1 != null) {
				if (!str1.equals(str2))
					return false;
			}

		} else if (val1 instanceof VectorStringSampleValue) {
			if (!(val2 instanceof VectorStringSampleValue))
				return false;

			VectorStringSampleValue vv1 = (VectorStringSampleValue) val1;
			VectorStringSampleValue vv2 = (VectorStringSampleValue) val2;

			String str1 = vv1.toString();
			String str2 = vv2.toString();
			if (!str1.equals(str2))
				return false;

		} else
			return false;

		return true;

	}
/**
 * check whether the two timeevent have the same time stamp
 * @param tempEvent1 time event 1
 * @param tempEvent2  time event 2
 * @return true if they have the same time stamps. Other wise ,false
 */
	private boolean isMatchingTimeStamp(final DBRTimeEvent tempEvent1,
			final DBRTimeEvent tempEvent2) {
		// Compare data type and value

		java.sql.Timestamp time1 = tempEvent1.getEventTimeStamp();
		java.sql.Timestamp time2 = tempEvent2.getEventTimeStamp();
		long miltime1 = time1.getTime();
		long miltime2 = time2.getTime();
		if (miltime1 != miltime2) {
			return false;
		} else
			return true;

	}

}
