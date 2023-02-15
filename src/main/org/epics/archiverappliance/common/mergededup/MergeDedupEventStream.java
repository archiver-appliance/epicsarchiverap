package org.epics.archiverappliance.common.mergededup;

import java.io.IOException;
import java.util.Iterator;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

/**
 * EventStream that is constructed with two source EventStream's ( for the same PV ) and then return's a merged-deduped stream of Events.
 * The order of the streams in the constructor is significant as the first stream is the preferred stream in case of conflicts.
 * For example, only the description from the first stream is used as the event description for this stream.
 * Similarly, if both streams contain an event with the same timestamp, the event from the first stream ( along with all metainfo ) is used.   
 * @author mshankar
 *
 */
public class MergeDedupEventStream implements EventStream, RemotableOverRaw {
	private static Logger logger = Logger.getLogger(MergeDedupEventStream.class);
	
	EventStream strm1;
	EventStream strm2;
	public MergeDedupEventStream(EventStream stream1, EventStream stream2) {
		this.strm1 = stream1;
		this.strm2 = stream2;
	}
	
	private class MGIterator implements Iterator<Event> {
		Iterator<Event> it1 = strm1.iterator();
		Iterator<Event> it2 = strm2.iterator();
		Event s1next = null;
		Event s2next = null;
		
		MGIterator() {
			if(it1 != null && it1.hasNext()) { s1next = it1.next(); } 
			if(it2 != null && it2.hasNext()) { s2next = it2.next(); } 
		}
		
		@Override
		public boolean hasNext() {			
			return s1next != null || s2next != null;
		}

		@Override
		public Event next() {
			Event ret = null;
			if (s1next != null && s2next != null ) {
				if(s1next.getEventTimeStamp().before(s2next.getEventTimeStamp())) {
					ret = s1next.makeClone();
					if(it1.hasNext()) { s1next = it1.next(); } else { s1next = null; }
				} else if (s1next.getEventTimeStamp().after(s2next.getEventTimeStamp())) {
					ret = s2next.makeClone();
					if(it2.hasNext()) { s2next = it2.next(); } else { s2next = null; }
				} else {
					ret = s1next.makeClone();
					if(it1.hasNext()) { s1next = it1.next(); } else { s1next = null; }
					if(it2.hasNext()) { s2next = it2.next(); } else { s2next = null; }
				}
			} else { 
				if (s1next != null) {
					logger.debug("S1 is done");
					ret = s1next.makeClone();
					if(it1.hasNext()) { s1next = it1.next(); } else { s1next = null; }
				} else if (s2next != null) {
					logger.debug("S2 is done");
					ret = s2next.makeClone();
					if(it2.hasNext()) { s2next = it2.next(); } else { s2next = null; }
				} else {
					throw new RuntimeException();  
				}
			}
			return ret;
		}
		
	}

	@Override
	public Iterator<Event> iterator() {
		return new MGIterator();
	}

	@Override
	public void close() throws IOException {
		try { this.strm1.close(); } catch(IOException ex) { logger.error(ex); }
		try { this.strm2.close(); } catch(IOException ex) { logger.error(ex); }
	}

	@Override
	public RemotableEventStreamDesc getDescription() {
		return ((RemotableOverRaw)strm1).getDescription();
	}

}
