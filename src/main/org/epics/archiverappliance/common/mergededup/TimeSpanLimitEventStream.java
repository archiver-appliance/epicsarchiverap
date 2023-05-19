package org.epics.archiverappliance.common.mergededup;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

/**
 * An EventStream that wraps another event stream but limits the data to samples greater than or equal to a specified start time and less than or equal to a specified end time.
 * Used for enforcing partition granularities.
 * @author mshankar
 *
 */
public class TimeSpanLimitEventStream implements EventStream, RemotableOverRaw {
	private static Logger logger = LogManager.getLogger(TimeSpanLimitEventStream.class);
	
	private EventStream srcStream;
	private Timestamp startTime;
	private Timestamp endTime;
	public TimeSpanLimitEventStream(EventStream srcStream, Timestamp startTime, Timestamp endTime) {
		this.srcStream = srcStream;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	public TimeSpanLimitEventStream(EventStream srcStream, long startEpoch, long endEpoch) {
		this.srcStream = srcStream;
		this.startTime = TimeUtils.convertFromEpochSeconds(startEpoch, 0);
		this.endTime = TimeUtils.convertFromEpochSeconds(endEpoch, 0);
	}
	
	private class MGIterator implements Iterator<Event> {		
		Iterator<Event> it = srcStream.iterator();
		Event event = null;
		
		MGIterator() {
			move();
		}
		
		@Override
		public boolean hasNext() {			
			return event != null || event != null;
		}

		@Override
		public Event next() {
			Event ret = null;
			if(event != null) {
				ret = event.makeClone();
				move();
			}
			return ret;
		}
		
		private void move() {
			event = null;
			if(it != null) {
				while(it.hasNext()) {
					Event nxtEvent = it.next();
					Timestamp ts = nxtEvent.getEventTimeStamp();
					if(ts.before(startTime) || ts.after(endTime)) {
						logger.warn("Skipping event outside the time range " + TimeUtils.convertToHumanReadableString(ts));						
					} else {
						// logger.debug("Event inside the time range " + TimeUtils.convertToHumanReadableString(ts));
						event = nxtEvent;
						break;
					}
				}
			}
		}
		
	}

	@Override
	public Iterator<Event> iterator() {
		return new MGIterator();
	}

	@Override
	public void close() throws IOException {
		try { this.srcStream.close(); } catch(IOException ex) { logger.error(ex); }
	}

	@Override
	public RemotableEventStreamDesc getDescription() {
		return (RemotableEventStreamDesc)srcStream.getDescription();
	}

}
