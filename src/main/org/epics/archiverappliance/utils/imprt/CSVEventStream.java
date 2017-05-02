/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.imprt;


import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.config.ArchDBRTypes;

/**
 * An event stream backed by a CSV file. 
 * CSV file format is the one used by Bob Hall for export from ChannelArchiver - EPICS epochseconds, nanos, value, status, severity.
 * Example: - 644223600,461147000,5.59054,0,0 
 * @author mshankar
 *
 */
public class CSVEventStream implements EventStream {
	private static Logger logger = Logger.getLogger(CSVEventStream.class.getName());
	private String csvFileName;
	private ArchDBRTypes dbrtype;
	private LinkedList<CSVEventStreamIterator> allIterators = new LinkedList<CSVEventStreamIterator>();
	private EventStreamDesc desc;
	
	/**
	 * Create a CSVEventStream backed by a file. As we cannot tell the DBR type from the file alone, we need to specify the DBR type as part of the constructor.
	 * @param pvName The name of PV.
	 * @param fileName   &emsp; 
	 * @param type ArchDBRTypes
	 */
	public CSVEventStream(String pvName, String fileName, ArchDBRTypes type) {
		this.csvFileName = fileName;
		this.dbrtype = type;
		desc = new EventStreamDesc(type, pvName);
	}

	@Override
	public Iterator<Event> iterator() {
		try {
			CSVEventStreamIterator ret = new CSVEventStreamIterator(this.csvFileName, this.dbrtype);
			allIterators.add(ret);
			return ret;
		} catch(Exception ex) {
			logger.error("Exception opening CSV file " + csvFileName, ex);
		}
		return null;
	}

	@Override
	public void close() {
		for(CSVEventStreamIterator it : allIterators) {
			it.close();
		}
	}

	@Override
	public EventStreamDesc getDescription() {
		return desc;
	}
}
