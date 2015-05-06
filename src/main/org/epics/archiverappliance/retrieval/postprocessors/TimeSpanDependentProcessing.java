package org.epics.archiverappliance.retrieval.postprocessors;

import java.util.List;

import org.epics.archiverappliance.common.TimeSpan;

/**
 * Post processors can optionally implement this interface if the implement timespan specific functionality
 * @author mshankar
 *
 */
public interface TimeSpanDependentProcessing {
	/**
	 * The data source resolution will call this method to give the post processor a chance to implement time span dependent post processing.
	 * @param timeSpans
	 * @return
	 */
	public List<TimeSpanDependentProcessor> generateTimeSpanDependentProcessors(List<TimeSpan> timeSpans);

}
