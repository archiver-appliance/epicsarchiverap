package org.epics.archiverappliance.retrieval.postprocessors;

import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.ChangeInYearsException;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

/**
 * An event stream that wraps an ArrayList event stream; use this if the data in your consolidated results do not fit a clean binning pattern and you need to return results that span multiple years.
 *
 * @author mshankar
 * @see PostProcessorWithConsolidatedEventStream
 */
public class ArrayListCollectorEventStream implements EventStream, RemotableOverRaw {
	private static Logger logger = Logger.getLogger(ArrayListCollectorEventStream.class.getName());
	private ArrayListEventStream sourceStream;
	private final RemotableEventStreamDesc desc;
	private Iterator<Event> theOneAndOnlyIterator;
	public ArrayListCollectorEventStream(ArrayListEventStream sourceStream) {
		this.sourceStream = sourceStream;
		this.desc = new RemotableEventStreamDesc(sourceStream.getDescription());
	}

	@Override
	public void close() throws IOException {
	}

	@Override
	public Iterator<Event> iterator() {
		if(theOneAndOnlyIterator != null) { 
			return theOneAndOnlyIterator;
		} else { 
			theOneAndOnlyIterator = new ArrayListCollectorEventStreamIterator(); 
			return theOneAndOnlyIterator;
		}
	}

	@Override
	public RemotableEventStreamDesc getDescription() {
		return desc;
	}

	private class ArrayListCollectorEventStreamIterator implements Iterator<Event> {
		int currentIndex = 0;
		short currentYear = -1;

		ArrayListCollectorEventStreamIterator() {
			currentIndex = 0;
		}
		
		@Override
		public boolean hasNext() {
			return currentIndex < sourceStream.size();
		}

		@Override
		public Event next() {
			Event next = sourceStream.get(currentIndex);
			short eventYear = TimeUtils.computeYearForEpochSeconds(next.getEpochSeconds());
			if(eventYear != currentYear) { 
				logger.info("Detected a change in years eventYear " + eventYear + " and currentYear is " + currentYear);
				ArrayListCollectorEventStream.this.desc.setYear(eventYear);
				short tempCurrentYear = currentYear;
				currentYear = eventYear;
				throw new ChangeInYearsException(tempCurrentYear, eventYear);
			}
			currentIndex++;
			return next;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		} 
	}
}

