package org.epics.archiverappliance.retrieval;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.postprocessors.PostProcessor;

/**
 * Encapsulates an event stream into a callable.
 * @author mshankar
 *
 */
public class CallableEventStream implements Callable<EventStream> {
	private static Logger logger = Logger.getLogger(CallableEventStream.class.getName());
	private EventStream theStream = null;
	
	public CallableEventStream(EventStream st) {
		this.theStream = st;
	}

	@Override
	public EventStream call() throws Exception {
		return theStream;
	}

	public static List<Callable<EventStream>> makeOneStreamCallableList(EventStream st) {
		List<Callable<EventStream>> ret = new ArrayList<Callable<EventStream>>();
		ret.add(new CallableEventStream(st));
		return ret;
	}
	
	public static List<Callable<EventStream>> makeOneStreamCallableList(EventStream st, PostProcessor postProcessor, boolean wrapWithPostProcessor) {
		List<Callable<EventStream>> ret = new ArrayList<Callable<EventStream>>();
		if(wrapWithPostProcessor) {
			ret.add(postProcessor.wrap(new CallableEventStream(st)));
		} else { 
			ret.add(new CallableEventStream(st));
		}
		return ret;
	}
	
	public static Callable<EventStream> makeOneStreamCallable(EventStream st, PostProcessor postProcessor, boolean wrapWithPostProcessor) {
		if(wrapWithPostProcessor) {
			return postProcessor.wrap(new CallableEventStream(st));
		} else { 
			logger.debug("Skipping wrapping for " + st.getDescription().getSource());
			return new CallableEventStream(st);
		}
	}

	public static Callable<EventStream> makeOneEventCallable(Event ev, RemotableEventStreamDesc desc, PostProcessor postProcessor, boolean wrapWithPostProcessor) {
		ArrayListEventStream strm = new ArrayListEventStream(1, desc);
		strm.add(ev);
		if(wrapWithPostProcessor) {
			return postProcessor.wrap(new CallableEventStream(strm));
		} else { 
			logger.debug("Skipping wrapping for " + strm.getDescription().getSource());
			return new CallableEventStream(strm);
		}
	}

}
