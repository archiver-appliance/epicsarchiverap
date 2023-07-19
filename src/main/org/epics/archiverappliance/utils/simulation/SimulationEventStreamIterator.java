/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.simulation;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator for the SimulationEventStream
 * @author mshankar
 *
 */
public class SimulationEventStreamIterator implements Iterator<Event> {
	private final ArchDBRTypes type;
	private final SimulationValueGenerator valueGenerator;
	private final int periodInSeconds;
	private final Instant end;
	private Instant currentTime;

	public SimulationEventStreamIterator(ArchDBRTypes type, SimulationValueGenerator valueGenerator, Instant start, Instant end, int periodInSeconds) {
		this.type = type;
		this.valueGenerator = valueGenerator;
		if (periodInSeconds <= 0) {
			throw new IndexOutOfBoundsException(periodInSeconds);
		}
		this.periodInSeconds = periodInSeconds;
		this.end = end;
		this.currentTime = start;
	}

	@Override
	public boolean hasNext() {
		return this.currentTime.isBefore(this.end);
	}

	@Override
	public Event next() {
		if (!hasNext()) {
			throw new NoSuchElementException("Iterator finished");
		}

		SimulationEvent simulationEvent = new SimulationEvent(this.currentTime, type, valueGenerator);
		currentTime = currentTime.plusSeconds(this.periodInSeconds);
		return simulationEvent;
	}

	@Override
	public void remove() {
		throw new RuntimeException("Not implemented");
	}

	public long getNumberOfEventsLeft() {
		return Duration.between(this.currentTime, this.end).getSeconds() / periodInSeconds;
	}
}
