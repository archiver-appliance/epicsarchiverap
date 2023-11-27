package org.epics.archiverappliance.common.mergededup;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

/**
 * An EventStream that wraps another event stream but limits the data to samples greater than or equal to a specified start time and less than or equal to a specified end time.
 * Used for enforcing partition granularities.
 * @author mshankar
 *
 */
public class TimeSpanLimitEventStream implements EventStream, RemotableOverRaw {
	private static final Logger logger = LogManager.getLogger(TimeSpanLimitEventStream.class);

	private final EventStream srcStream;
	private final Instant startTime;
	private final Instant endTime;

	public TimeSpanLimitEventStream(EventStream srcStream, Instant startTime, Instant endTime) {
		this.srcStream = srcStream;
		this.startTime = startTime;
		this.endTime = endTime;
	}

	private class MGIterator implements Iterator<Event> {
		Iterator<Event> it = srcStream.iterator();
		Event event = null;
		
		MGIterator() {
			move();
		}
		
		@Override
		public boolean hasNext() {
			return event != null;
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
					Instant ts = nxtEvent.getEventTimeStamp();
					if (ts.isBefore(startTime) || ts.isAfter(endTime)) {
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
