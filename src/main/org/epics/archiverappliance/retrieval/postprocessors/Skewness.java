package org.epics.archiverappliance.retrieval.postprocessors;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.epics.archiverappliance.Event;

/**
 * Implements the skewness over a bin. 
 * For example, skewness_600(PV) returns DescriptiveStatistics.getSkewness() with a bin size of 600 seconds
 * This uses DescriptiveStatistics which stores values; so, this will throw OutOfMemoryExceptions if the bin sizes are large
 * @author mshankar
 *
 */
public class Skewness extends SummaryStatsPostProcessor implements PostProcessor {
	static final String IDENTITY = "skewness";

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
				return stats.getSkewness();
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
