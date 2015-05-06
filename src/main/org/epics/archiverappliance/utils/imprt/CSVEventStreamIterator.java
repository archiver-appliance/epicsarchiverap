/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.imprt;

import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.lang.reflect.Constructor;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import au.com.bytecode.opencsv.CSVReader;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;

/**
 * The iterator for a CSV backed event stream
 * @author mshankar
 *
 */
class CSVEventStreamIterator implements Iterator<Event> {
	private static Logger logger = Logger.getLogger(CSVEventStreamIterator.class.getName());
	private String csvFileName;
	private ArchDBRTypes dbrtype;
	private LineNumberReader linereader;
	private CSVReader csvreader;
	private Event nextEvent;
	private Constructor<? extends DBRTimeEvent> eventConstructor;
	
	public CSVEventStreamIterator(String fileName, ArchDBRTypes type) throws IOException {
		this.csvFileName = fileName;
		this.dbrtype = type;
		linereader = new LineNumberReader(new FileReader(fileName));
		csvreader = new CSVReader(linereader);
		eventConstructor = DBR2PBTypeMapping.getPBClassFor(dbrtype).getSerializingConstructor();
		assert(eventConstructor != null);
	}

	@Override
	public boolean hasNext() {
		nextEvent = readNextEvent();
		if(nextEvent != null) return true;
		return false;
	}

	@Override
	public Event next() {
		return nextEvent;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("We do not support the remove method in this iterator");
	}

	public void close() {
		try { csvreader.close(); csvreader = null; } catch (Exception ex) { } 
	}
	
	private Event readNextEvent() {
		try { 
			String [] line = csvreader.readNext();
			if(line == null || line.length < 5) return null;
			CSVEvent csvEvent = new CSVEvent(line, dbrtype);
			return (Event) eventConstructor.newInstance(csvEvent);
		} catch(Exception ex) {
			logger.error("Exception parsing CSV file " + csvFileName + " in line " + (linereader.getLineNumber()-1), ex);
		}
		return null;
	}
}