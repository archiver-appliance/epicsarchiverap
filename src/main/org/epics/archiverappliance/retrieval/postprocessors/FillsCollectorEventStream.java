package org.epics.archiverappliance.retrieval.postprocessors;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.data.AlarmInfo;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.ChangeInYearsException;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

/**
 * This is similar to the summary stats collector except we keep aspects of the original event stream like dbr_type and so on.
 *
 * @author mshankar
 * @see PostProcessorWithConsolidatedEventStream
 */
public class FillsCollectorEventStream implements EventStream, RemotableOverRaw {
	private static Logger logger = Logger.getLogger(FillsCollectorEventStream.class.getName());
	private final RemotableEventStreamDesc desc;
	private LinkedHashMap<Long, Event> consolidatedData;
	private long firstBin;
	private long lastBin;
	private int intervalSecs;
	private Iterator<Event> theOneAndOnlyIterator;
	private boolean fillOperator = true;
	public FillsCollectorEventStream(long firstBin, long lastBin, int intervalSecs, RemotableEventStreamDesc desc, LinkedHashMap<Long, Event> consolidatedData) {
		this.firstBin = firstBin;
		this.lastBin = lastBin;
		this.intervalSecs = intervalSecs;
		this.desc = new RemotableEventStreamDesc(desc);
		this.consolidatedData = consolidatedData;
	}

	public FillsCollectorEventStream(long firstBin, long lastBin, int intervalSecs, RemotableEventStreamDesc desc, LinkedHashMap<Long, Event> consolidatedData, boolean fillOperator) {
		this(firstBin,lastBin,intervalSecs,desc,consolidatedData);
		this.fillOperator = fillOperator;
	}

	
	@Override
	public void close() throws IOException {
	}

	@Override
	public Iterator<Event> iterator() {
		if(theOneAndOnlyIterator != null) { 
			return theOneAndOnlyIterator;
		} else { 
			theOneAndOnlyIterator = new FillsCollectorEventStreamIterator(); 
			return theOneAndOnlyIterator;
		}
	}

	@Override
	public RemotableEventStreamDesc getDescription() {
		return desc;
	}

	private class FillsCollectorEventStreamIterator implements Iterator<Event> {
		ArrayListEventStream strm = new ArrayListEventStream(consolidatedData.size(), desc);
		short currentYear = -1;
		int currentEvent = 0;
		int totalEvents = -1;
		FillsCollectorEventStreamIterator() {
			Event currentEvent = null;
			for(long binNum = firstBin; binNum <= lastBin; binNum++) {
				if(consolidatedData.containsKey(binNum)) {
					currentEvent = consolidatedData.get(binNum);
				} else {
					if(fillOperator) {
						if(currentEvent != null)  { 
							logger.debug("Inheriting previous value for bin " + binNum);
						}
					} else { 
						logger.debug("For non-fill operators, we do not inherit the previous bin's value " + binNum);
						currentEvent = null;
					}
				}
				if(currentEvent != null) { 
					long epochSeconds = binNum*intervalSecs + intervalSecs/2;
					Timestamp eventTs = null;
					if(fillOperator) {
						logger.debug("For fill operators, we put the event time stamp in the center of the bin.");
						eventTs = TimeUtils.convertFromEpochSeconds(epochSeconds, 0);
					} else {
						logger.debug("This non-fill operators, we use the event's timestamp as is");
						eventTs = currentEvent.getEventTimeStamp();

					}
					POJOEvent pojoEvent = new POJOEvent(desc.getArchDBRType(),
							eventTs, 
							currentEvent.getSampleValue(), 
							((AlarmInfo)currentEvent).getStatus(), 
							((AlarmInfo)currentEvent).getSeverity());
					strm.add(pojoEvent.makeClone());
					if(currentYear == -1) { 
						// Initialize the current year as the year of the first bin with a value it it.
						currentYear = TimeUtils.computeYearForEpochSeconds(epochSeconds);
						FillsCollectorEventStream.this.desc.setYear(currentYear);
					}
				}
			}
			
			totalEvents = strm.size();
		}
		
		@Override
		public boolean hasNext() {
			return currentEvent < totalEvents;
		}

		@Override
		public Event next() {
			Event next = strm.get(currentEvent);
			short eventYear = TimeUtils.computeYearForEpochSeconds(next.getEpochSeconds());
			if(eventYear != currentYear) { 
				logger.info("Detected a change in years eventYear " + eventYear + " and currentYear is " + eventYear);
				FillsCollectorEventStream.this.desc.setYear(eventYear);
				short tempCurrentYear = currentYear;
				currentYear = eventYear;
				throw new ChangeInYearsException(tempCurrentYear, eventYear);
			}
			currentEvent++;
			return next;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		} 
	}
}

