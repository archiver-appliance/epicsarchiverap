package org.epics.archiverappliance.retrieval.postprocessors;

import java.util.HashMap;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;

/**
 * Similar to the mean operator; in additon, the stdz is passed in as an extra column
 * @author mshankar
 *
 */
public class ErrorBars extends SummaryStatsPostProcessor implements PostProcessor {
	static final String IDENTITY = "errorbar";
	private static Logger logger = LogManager.getLogger(ErrorBars.class.getName());

	@Override
	public String getIdentity() {
		return IDENTITY;
	}

	@Override
	public SummaryStatsCollector getCollector() {
		return new SummaryStatsCollectorAdditionalColumns() {
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

			@Override
			public HashMap<String, String> getAdditionalStats() {
				HashMap<String, String> additionalStats = new HashMap<String, String>();
				additionalStats.put("stdz", Double.toString(stats.getStandardDeviation()));
				return additionalStats;
			}
		};
	}
}
