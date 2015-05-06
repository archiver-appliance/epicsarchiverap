package org.epics.archiverappliance.retrieval.workers;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

/**
 * Expands the results from a Reader using the current thread.
 * This is the default strategy for most retrievals; especially if the data sizes are large; here, we leverage streaming to minimize memory consumption.
 * We only support one iterator for this stream.
 * @author mshankar
 *
 */
public class CurrentThreadWorkerEventStream implements EventStream, RemotableOverRaw {
	private static Logger logger = Logger.getLogger(CurrentThreadWorkerEventStream.class.getName());
	private String pvName;
	private List<Callable<EventStream>> theStreams = null;
	private CurrentThreadWorkerEventStreamIterator theIterator = null;
	
	
	public CurrentThreadWorkerEventStream(String pvName, List<Callable<EventStream>> streams) {
		this.pvName = pvName;
		this.theStreams = streams;
		theIterator = new CurrentThreadWorkerEventStreamIterator();
	}

	@Override
	public void close() throws IOException {
		if(theIterator != null) { 
			theIterator.close();
			theIterator = null;
		}
	}

	@Override
	public Iterator<Event> iterator() {
		// We only support one iterator out of this stream.
		return theIterator;
	}

	@Override
	public RemotableEventStreamDesc getDescription() {
		return ((RemotableOverRaw)theIterator.currStream).getDescription();
	}

	private class CurrentThreadWorkerEventStreamIterator implements Iterator<Event>, AutoCloseable {
		private Event nextEvent = null;
		private int currentStreamIndex = 0;
		private EventStream currStream = null;
		private Iterator<Event> currStreamIterator = null;
		
		public CurrentThreadWorkerEventStreamIterator() {
			try {
				if(theStreams != null && theStreams.size() > 0) {
					currStream = theStreams.get(currentStreamIndex).call();
					currStreamIterator = currStream.iterator();
				}
			} catch(Exception ex) {
				logger.error("Exception fetching events from stream for pv " + pvName, ex);
			}
		}
		
		@Override
		public boolean hasNext() {
			if(currStream == null || currStreamIterator == null) return false;
			nextEvent = fetchNextEvent();
			return (nextEvent != null);
		}

		@Override
		public Event next() {
			return nextEvent;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void close() throws IOException {
			nextEvent = null;
			if(currStream != null) currStream.close();
			currStream = null;
			currStreamIterator = null;
		}
		
		private Event fetchNextEvent() {
			try {
				for(int infiniteloopprevention = 0; infiniteloopprevention < theStreams.size(); infiniteloopprevention++) {
					if(currStreamIterator.hasNext()) {
						return currStreamIterator.next();
					}
					if(currStream != null) {
						currStream.close();
						currStream = null;
					}
					currStreamIterator = null;

					currentStreamIndex++;
					if(currentStreamIndex >= theStreams.size()) {
						return null;
					}

					currStream = theStreams.get(currentStreamIndex).call();
					currStreamIterator = currStream.iterator();
				}
			} catch(Exception ex) {
				logger.error("Exception fetching events from stream for pv " + pvName, ex);
			}
			return null;
		}
	}
}
