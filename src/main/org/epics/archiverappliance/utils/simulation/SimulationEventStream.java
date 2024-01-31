/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.simulation;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;

/**
 * An EventStream that backed by a generator.
 * @author mshankar
 *
 */
public class SimulationEventStream implements EventStream, RemotableOverRaw {
    private final ArchDBRTypes type;
    private final SimulationValueGenerator valueGenerator;
    private final Instant start;
    private final Instant end;
    private final int periodInSeconds;

    public SimulationEventStream(
            ArchDBRTypes type,
            SimulationValueGenerator valueGenerator, Instant start, Instant end,
            int periodInSeconds) {
        this.type = type;
        this.valueGenerator = valueGenerator;
        this.start = start;
        this.end = end;
        this.periodInSeconds = periodInSeconds;
    }


    @Override
    public Iterator<Event> iterator() {
        return new SimulationEventStreamIterator(type, valueGenerator, start, end, periodInSeconds);
    }

    @Override
    public void close() {
        // Nothing to do here...
    }

    @Override
    public RemotableEventStreamDesc getDescription() {
        return new RemotableEventStreamDesc(type, "Simulation", TimeUtils.getCurrentYear());
    }

    public long getNumberOfEvents() {
        return (Duration.between(this.start, this.end).getSeconds() + 1) / periodInSeconds;
    }
}
