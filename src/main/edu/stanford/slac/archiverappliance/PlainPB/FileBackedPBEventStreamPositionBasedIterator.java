/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;


import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Path;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;

/**
 * An iterator for a FileBackedPBEventStream.
 * @author mshankar
 *
 */
public class FileBackedPBEventStreamPositionBasedIterator implements FileBackedPBEventStreamIterator {
	private static Logger logger = Logger.getLogger(FileBackedPBEventStreamPositionBasedIterator.class.getName());
	private long startFilePos = 0;
	private long endFilePos = 0;
	private short year = 0;
	private LineByteStream lbs = null;
	private ByteArray nextLine = new ByteArray(LineByteStream.MAX_LINE_SIZE);
	private ArchDBRTypes type;
	private DBR2PBTypeMapping mapping;
	private Constructor<? extends DBRTimeEvent> unmarshallingConstructor;
	

	public FileBackedPBEventStreamPositionBasedIterator(Path path, long startFilePos, long endFilePos, short year, ArchDBRTypes type) throws IOException {
		this.startFilePos = startFilePos;
		this.endFilePos = endFilePos;
		this.type = type;
		mapping = DBR2PBTypeMapping.getPBClassFor(this.type);
		unmarshallingConstructor = mapping.getUnmarshallingFromByteArrayConstructor();
		assert(startFilePos >= 0);
		assert(endFilePos >= 0);
		assert(endFilePos >= startFilePos);
		this.year = year;
		lbs = new LineByteStream(path, this.startFilePos, this.endFilePos);
		lbs.seekToFirstNewLine();
	}

	
	@Override
	public boolean hasNext() {
		try {
			lbs.readLine(nextLine);
			if(!nextLine.isEmpty()) return true;
		} catch(Exception ex) {
			logger.error("Exception creating event object", ex);
		}
		return false;
	}


	@Override
	public Event next() {
		try {
			return (Event) unmarshallingConstructor.newInstance(year, nextLine);
		} catch (Exception ex) {
			logger.error("Exception creating event object", ex);
			return null;
		}
	}


	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	public void close() {
		lbs.safeClose();
	}
}
