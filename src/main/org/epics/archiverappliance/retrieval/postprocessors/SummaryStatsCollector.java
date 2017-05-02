package org.epics.archiverappliance.retrieval.postprocessors;

import org.epics.archiverappliance.Event;

/**
 * The SummaryStatsPostProcessor uses instances of this interface to compute statistical summaries 
 * @author mshankar
 */
public interface SummaryStatsCollector {
	public void setBinParams(int intervalSecs, long binNum);
	/**
	 * Add an event to the collector
	 * It is quite possible that this is called from multiple threads.
	 * @param e Event
	 */
	public void addEvent(Event e);
	/**
	 * Have any events been added? If not, we inherit the previous value.
	 * @return boolean True or False
	 */
	public boolean haveEventsBeenAdded();
	/**
	 * Get the statistic
	 * @return the statistic
	 */
	public double getStat();
}
