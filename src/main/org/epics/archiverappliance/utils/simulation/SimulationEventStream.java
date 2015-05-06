/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.simulation;

import java.util.Iterator;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

/**
 * An EventStream that backed by a generator. 
 * @author mshankar
 *
 */
public class SimulationEventStream implements EventStream, RemotableOverRaw {
	private ArchDBRTypes type;
	private SimulationValueGenerator valueGenerator;
	private short startyear;
	private short endyear;
	
	public SimulationEventStream(ArchDBRTypes type, SimulationValueGenerator valueGenerator) {
		this.type = type;
		this.valueGenerator = valueGenerator;
		this.startyear = TimeUtils.getCurrentYear();
		this.endyear = this.startyear;
	}
	
	public SimulationEventStream(ArchDBRTypes type, SimulationValueGenerator valueGenerator, short year) {
		this.type = type;
		this.valueGenerator = valueGenerator;
		this.startyear = year;
		this.endyear = this.startyear;
	}
	
	public SimulationEventStream(ArchDBRTypes type, SimulationValueGenerator valueGenerator, short startyear, short endyear) {
		this.type = type;
		this.valueGenerator = valueGenerator;
		this.startyear = startyear;
		this.endyear = endyear;
	}



	@Override
	public Iterator<Event> iterator() {
		return new SimulationEventStreamIterator(type, valueGenerator, startyear, endyear);
	}
	
	@Override
	public void close() {
		// Nothing to do here...
	}

	@Override
	public RemotableEventStreamDesc getDescription() {
		return new RemotableEventStreamDesc(type, "Simulation", TimeUtils.getCurrentYear());
	}
	
	
}
