package org.epics.archiverappliance.retrieval.postprocessors;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;

/**
 * Implements the arithmetic mean across an interval
 * @author mshankar
 *
 */
public class Mean extends SummaryStatsPostProcessor implements PostProcessor {
	static final String IDENTITY = "mean";
	private static Logger logger = Logger.getLogger(Mean.class.getName());

	@Override
	public String getIdentity() {
		return IDENTITY;
	}

	@Override
	public SummaryStatsCollector getCollector() {
		return new SummaryStatsCollector() {
			SummaryStatistics stats = new SummaryStatistics();
			@Override
			public void setBinParams(int intervalSecs, long binNum) {
			}
			
			@Override
			public boolean haveEventsBeenAdded() {
				return stats.getN() > 0;
			}
			
			@Override
			public double getStat() {
				return stats.getMean();
			}
			
			@Override
			public void addEvent(Event e) {
				double val = e.getSampleValue().getValue().doubleValue();
				if(!Double.isNaN(val)) { 
					stats.addValue(val);
				} else { 
					logger.warn("Skipping NAN");
				}
			}
		};
	}
}
