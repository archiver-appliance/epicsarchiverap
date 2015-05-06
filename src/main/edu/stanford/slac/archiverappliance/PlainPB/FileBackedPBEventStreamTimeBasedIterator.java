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
import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;

/**
 * An iterator for a FileBackedPBEventStream.
 * @author mshankar
 *
 */
public class FileBackedPBEventStreamTimeBasedIterator implements FileBackedPBEventStreamIterator {
	private static Logger logger = Logger.getLogger(FileBackedPBEventStreamTimeBasedIterator.class.getName());
	private long startTimeEpochSeconds = 0;
	private long endTimeEpochSeconds = 0;
	private short year;
	private LineByteStream lbs = null;
	private ArchDBRTypes type;
	private DBR2PBTypeMapping mapping;
	private Constructor<? extends DBRTimeEvent> unmarshallingConstructor;
	Events events = new Events();
	
	private class Events { 
		private ByteArray line1 = new ByteArray(LineByteStream.MAX_LINE_SIZE);
		private Event event1 = null;
		private ByteArray line2 = new ByteArray(LineByteStream.MAX_LINE_SIZE);
		private Event event2 = null;
		
		void readEvents(LineByteStream lbs) throws Exception {
			if(event1 == null) {
				lbs.readLine(line1);
				if(!line1.isEmpty()) {
					event1 = (Event) unmarshallingConstructor.newInstance(year, line1);
					long event1EpochSeconds = event1.getEpochSeconds();
					if(event1EpochSeconds > endTimeEpochSeconds) { 
						event1 = null;
						line1.reset();
						return;
					}
				}
			}
			
			if(event2 == null) {
				lbs.readLine(line2);
				if(!line2.isEmpty()) {
					event2 = (Event) unmarshallingConstructor.newInstance(year, line2);
					long event2EpochSeconds = event2.getEpochSeconds();
					if(event2EpochSeconds >= endTimeEpochSeconds) { 
						event2 = null;
						line2.reset();
						return;
					}
				}
			}
		}
		
		boolean startFound() {
			if(event1 != null && event2 != null) {
				long event1EpochSeconds = event1.getEpochSeconds();
				long event2EpochSeconds = event2.getEpochSeconds();
				if(event1EpochSeconds >= startTimeEpochSeconds) {
					logger.debug("We have reached an event whose start time is greater than the requested start already. Terminating the search.");
					return true;
				}
				if(event1EpochSeconds < startTimeEpochSeconds && event2EpochSeconds >= startTimeEpochSeconds) return true;
				return false;
			}
			
			if(event1 != null) {
				assert(event2 == null);
				long event1EpochSeconds = event1.getEpochSeconds();
				logger.debug("Only one event found. As long as this is before the end time, we claim we found something.");
				if(event1EpochSeconds <= endTimeEpochSeconds) return true;
				return false;
			}
			
			return false;
		}
		
		Event popEvent() {
			Event previousEvent = event1;
			ByteArray previousByteArray = line1;
			if(event2 != null) { 
				event1 = event2;
				line1 = line2;
				event2 = null;
				line2 = previousByteArray;
			} else { 
				event1 = null;
			}
			return previousEvent;
		}
		
		boolean isEmpty() { 
			return event1 == null;
		}
		
		void clear() { 
			event1 = null;
			event2 = null;
			line1.reset();
			line2.reset();
		}
	}
	

	public FileBackedPBEventStreamTimeBasedIterator(Path path, Timestamp startTime, Timestamp endTime, short year, ArchDBRTypes type) throws IOException {
		this.startTimeEpochSeconds = TimeUtils.convertToEpochSeconds(startTime);
		this.endTimeEpochSeconds = TimeUtils.convertToEpochSeconds(endTime);
		this.type = type;
		mapping = DBR2PBTypeMapping.getPBClassFor(this.type);
		unmarshallingConstructor = mapping.getUnmarshallingFromByteArrayConstructor();
		assert(startTimeEpochSeconds >= 0);
		assert(endTimeEpochSeconds >= 0);
		assert(endTimeEpochSeconds >= startTimeEpochSeconds);
		this.year = year;
		lbs = new LineByteStream(path);
		try {
			lbs.readLine(events.line1); // This should read the header..
			events.readEvents(lbs);
			while(!events.isEmpty() && !events.startFound()) {
				events.popEvent();
				events.readEvents(lbs);
			}
		} catch(Exception ex) {
			logger.error("Exception getting next event from path " + path.toString(), ex);
			events.clear();
		}
	}

	
	@Override
	public boolean hasNext() {
		if(!events.isEmpty()) return true;
		
		try {
			events.readEvents(lbs);
			if(!events.isEmpty()) return true;
		} catch(Exception ex) {
			logger.error("Exception creating event object", ex);
		}
		return false;
	}


	@Override
	public Event next() {
		Event retVal = events.popEvent();
		return retVal;
	}


	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	public void close() {
		lbs.safeClose();
	}
}
