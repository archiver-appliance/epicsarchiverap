package org.epics.archiverappliance.retrieval.postprocessors;

import java.util.LinkedList;
import java.util.List;

import org.epics.archiverappliance.common.TimeSpan;

/**
 * A TimeSpan + PostProcessor used for time dependent sparsification.
 * @author mshankar
 *
 */
public class TimeSpanDependentProcessor {
	private TimeSpan timeSpan;
	private PostProcessor postProcessor;
	
	public TimeSpanDependentProcessor(TimeSpan timeSpan, PostProcessor postProcessor) {
		this.timeSpan = timeSpan;
		this.postProcessor = postProcessor;
	}
	
	public TimeSpan getTimeSpan() {
		return timeSpan;
	}
	
	public PostProcessor getPostProcessor() {
		return postProcessor;
	}
	
	
	public static List<TimeSpanDependentProcessor> sameProcessorForAllTimeSpans(List<TimeSpan> timeSpans, PostProcessor postProcessor) { 
		LinkedList<TimeSpanDependentProcessor> ret = new LinkedList<TimeSpanDependentProcessor>();
		for(TimeSpan timeSpan : timeSpans) { 
			ret.add(new TimeSpanDependentProcessor(timeSpan, postProcessor));
		}
		return ret;
	}

}
