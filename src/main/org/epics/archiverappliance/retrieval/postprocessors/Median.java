package org.epics.archiverappliance.retrieval.postprocessors;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.epics.archiverappliance.Event;

/**
 * Implements the median over a bin. 
 * For example, median_600(PV) returns DescriptiveStatistics.getPercentile(50) with a bin size of 600 seconds
 * This uses DescriptiveStatistics which stores values; so, this will throw OutOfMemoryExceptions if the bin sizes are large
 * @author mshankar
 *
 */
public class Median extends SummaryStatsPostProcessor implements PostProcessor {
	static final String IDENTITY = "median";

	@Override
	public String getIdentity() {
		return IDENTITY;
	}

	@Override
	public SummaryStatsCollector getCollector() {
		return new SummaryStatsCollector() {
			DescriptiveStatistics stats = new DescriptiveStatistics();
			@Override
			public void setBinParams(int intervalSecs, long binNum) {
			}
			
			@Override
			public boolean haveEventsBeenAdded() {
				return stats.getN() > 0;
			}
			
			@Override
			public double getStat() {
				return stats.getPercentile(50);
			}
			
			@Override
			public void addEvent(Event e) {
				double val = e.getSampleValue().getValue().doubleValue();
				if(!Double.isNaN(val)) { 
					stats.addValue(val);
				}
			}
		};
	}
}
