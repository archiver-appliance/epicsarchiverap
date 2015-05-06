/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.simulation;

import java.util.GregorianCalendar;
import java.util.Iterator;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;

/**
 * Iterator for the SimulationEventStream
 * @author mshankar
 *
 */
public class SimulationEventStreamIterator implements Iterator<Event> {
	int currentseconds = 0;
	short currentyear;
	public static int DEFAULT_NUMBER_OF_SAMPLES = 60*60*24*365; 
	public static int LEAPYEAR_NUMBER_OF_SAMPLES = 60*60*24*366; 
	private int numberofsamples = DEFAULT_NUMBER_OF_SAMPLES;
	private short startyear;
	private short endyear;
	private boolean wechoosethenumberofsamples = false;
	private ArchDBRTypes type;
	private SimulationValueGenerator valueGenerator;
	
	public SimulationEventStreamIterator(ArchDBRTypes type, SimulationValueGenerator valueGenerator, short startyear, short endyear) {
		this.type = type;
		this.valueGenerator = valueGenerator;
		this.numberofsamples = valueGenerator.getNumberOfSamples(type);
		if(this.numberofsamples < 0) {
			this.wechoosethenumberofsamples = true;
			this.numberofsamples = DEFAULT_NUMBER_OF_SAMPLES;
		}
		this.startyear = startyear;
		this.endyear = endyear;
		setCurrentYear(this.startyear);
		assert(this.numberofsamples > 0);
	}

	@Override
	public boolean hasNext() {
		boolean moreLeftInCurrentYear = (currentseconds < numberofsamples);
		if(moreLeftInCurrentYear) return true;
		if(this.currentyear < this.endyear) {
			setCurrentYear((short) (this.currentyear+1));
			currentseconds = 0;
			return true;
		}
		
		return false;
	}

	@Override
	public Event next() {
		return new SimulationEvent(currentseconds++, currentyear, type, valueGenerator);
	}

	@Override
	public void remove() {
		throw new RuntimeException("Not implemented");
	}
	
	private void setCurrentYear(short year) {
		this.currentyear = year;
		if(wechoosethenumberofsamples) {
			// In this case, we make the decision on the number of samples
			// This is based on the year.
			GregorianCalendar cal = new GregorianCalendar();
			if(cal.isLeapYear(this.currentyear)) {
				numberofsamples = LEAPYEAR_NUMBER_OF_SAMPLES;
			} else {
				numberofsamples = DEFAULT_NUMBER_OF_SAMPLES;
			}
		}
	}

}
