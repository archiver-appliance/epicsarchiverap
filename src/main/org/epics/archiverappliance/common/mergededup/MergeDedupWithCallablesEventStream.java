package org.epics.archiverappliance.common.mergededup;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.retrieval.CallableEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;

/**
 * This is almost idential to the MergeDedupEventStream; expect it takes as its first argument a list of EventStream callables 
 * which it then merges with the second EventStream    
 * @author mshankar
 *
 */
public class MergeDedupWithCallablesEventStream implements EventStream, RemotableOverRaw {
	private static Logger logger = Logger.getLogger(MergeDedupWithCallablesEventStream.class);
	
	List<Callable<EventStream>> callables1;
	List<Callable<EventStream>> callables2;
	RemotableEventStreamDesc description;
	int evCount1, evCount2;
	MGIterator theIterator;
	
	public MergeDedupWithCallablesEventStream(List<Callable<EventStream>> clbs1, List<Callable<EventStream>> clbs2) {
		this.callables1 = clbs1;
		this.callables2 = clbs2;
		this.evCount1 = 0;
		this.evCount2 = 0;
		theIterator = new MGIterator();
	}

	public MergeDedupWithCallablesEventStream(List<Callable<EventStream>> clbs, EventStream stream2, PostProcessor postProcessor) {
		this.callables1 = clbs;
		this.callables2 = new LinkedList<Callable<EventStream>>();
		callables2.add(CallableEventStream.makeOneStreamCallable(stream2, postProcessor, (postProcessor != null)));
		theIterator = new MGIterator();
	}
	
	private class MGIterator implements Iterator<Event> {
		Iterator<Callable<EventStream>> citer1;
		Iterator<Callable<EventStream>> citer2;
		EventStream strm1 = null;
		EventStream strm2 = null;
		Iterator<Event> it1;
		Iterator<Event> it2;
		Event s1next = null;
		Event s2next = null;
		
		MGIterator() {
			citer1 = callables1.iterator();
			citer2 = callables2.iterator();
			moveIt1();
			moveIt2();
		}
				
		@Override
		public boolean hasNext() {			
			return s1next != null || s2next != null;
		}

		@Override
		public Event next() {
			Event ret = null;
			if (s1next != null && s2next != null ) {
				logger.debug("Still merging both streams " + TimeUtils.convertToHumanReadableString(s1next.getEventTimeStamp()) + " and " + TimeUtils.convertToHumanReadableString(s2next.getEventTimeStamp()));
				if(s1next.getEventTimeStamp().before(s2next.getEventTimeStamp())) {
					ret = s1next.makeClone();
					moveIt1();
				} else if (s1next.getEventTimeStamp().after(s2next.getEventTimeStamp())) {
					ret = s2next.makeClone();
					moveIt2();
				} else {
					ret = s1next.makeClone();
					moveIt1();
					moveIt2();
				}
			} else { 
				if (s1next != null) {
					logger.debug("S1 is done");
					ret = s1next.makeClone();
					moveIt1();
				} else if (s2next != null) {
					logger.debug("S2 is done");
					ret = s2next.makeClone();
					moveIt2();
				} else {
					throw new RuntimeException();  
				}
			}
			return ret;
		}

		private void moveIt1() {
			if(it1 != null && it1.hasNext()) {
				s1next = it1.next();
				evCount1++;
			} else { 
				s1next = null; 
				try {
					try { if(this.strm1 != null) { this.strm1.close(); this.strm1 = null; } } catch(IOException ex) { logger.error(ex); }
					if (citer1.hasNext()) {
						strm1 = citer1.next().call();
					}
				} catch(Exception ex) {
					logger.error("Exception getting data from primary stream", ex);
					strm1 = null;
				}
				if(strm1 != null) {
					description = (RemotableEventStreamDesc) strm1.getDescription();
					it1 = strm1.iterator();
					if(it1 != null && it1.hasNext()) { 
						s1next = it1.next();
						evCount1++;
					}
				}
			}
		}
		
		private void moveIt2() {
			if(it2 != null && it2.hasNext()) {
				s2next = it2.next(); 
				evCount2++;
			} else { 
				s2next = null; 
				try {
					try { if(this.strm2 != null) { this.strm2.close(); this.strm2 = null; } } catch(IOException ex) { logger.error(ex); }
					if (citer2.hasNext()) {
						strm2 = citer2.next().call();
					}
				} catch(Exception ex) {
					logger.error("Exception getting data from primary stream", ex);
					strm2 = null;
				}
				if(strm2 != null) {
					it2 = strm2.iterator();				
					if(it2 != null && it2.hasNext()) { 
						s2next = it2.next();
						evCount2++;
					}
				}
			}
		}
	}

	@Override
	public Iterator<Event> iterator() {
		return theIterator;
	}

	@Override
	public void close() throws IOException {
		logger.debug("Merged " + evCount1 + " and " + evCount2 + " events");
	}

	@Override
	public RemotableEventStreamDesc getDescription() {
		return description;
	}

}
