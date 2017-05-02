package org.epics.archiverappliance.retrieval.postprocessors;

import java.util.HashMap;

/**
 * The SummaryStatsPostProcessor uses instances of this interface to serve additional columns.
 * For example, the ErrorBars uses this to send mean and stdz as part of the response. 
 * @author mshankar
 */
public interface SummaryStatsCollectorAdditionalColumns extends SummaryStatsCollector {
	/**
	 * Get any additional statistics as a map of string &rarr; string
	 * @return HashMap additional Statistics 
	 */
	public HashMap<String, String> getAdditionalStats();
}
