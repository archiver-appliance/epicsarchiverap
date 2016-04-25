package org.epics.archiverappliance.retrieval.postprocessors;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

/**
 * Ignores data that is more than the specified amount of std deviation from the mean.
 * This is still a binned operation; however, it is different from the SummaryStatsPostProcessor as it does not reduce data significantly.
 * <pre>
Ignore flier could also be binned:
-Given vector X as a subset of data, say 300 seconds, N is a real number.
m = mean(X)
s = std(X)
index = index of points there X &gt; (abs(X) + N * s) 
i.e. index points to the values that are N standard deviations away from the mean.
</pre>
 * @author mshankar
 *
 */
public class IgnoreFliers implements PostProcessor {
	private static final float DEFAULT_DEVIATIONS = 3.0f;
	private static Logger logger = Logger.getLogger(IgnoreFliers.class.getName());
	int intervalSecs = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;
	float numDeviations = DEFAULT_DEVIATIONS;
	
	@Override
	public void initialize(String userarg, String pvName) throws IOException {
		try { 
			if(userarg != null && userarg.contains("_")) {
				String[] userparams = userarg.split("_");
				String intervalStr = userparams[1];
				intervalSecs = Integer.parseInt(intervalStr);
				if(userparams.length > 2) { 
					String numDeviationsStr = userparams[2];
					numDeviations = Float.parseFloat(numDeviationsStr);
				}
				logger.debug("Using user supplied interval of " + intervalSecs + " and " + numDeviations);
			} else {
				logger.debug("Using the default interval of  " + intervalSecs + " and " + numDeviations + " deviations as the user has not specified the interval argument.");
			}
		} catch(Exception ex) {
			throw new IOException(ex);
		}
	}

	@Override
	public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Timestamp start, Timestamp end, HttpServletRequest req) {
		float storageRate = typeInfo.getComputedStorageRate();
		long numSeconds = TimeUtils.convertToEpochSeconds(end) - TimeUtils.convertToEpochSeconds(start);
		// Add a fudge factor of 3; we generate one event for each event
		long estimatedMemoryConsumption = (long) (storageRate*numSeconds*2);
		return estimatedMemoryConsumption;
	}
	
	@Override
	public Callable<EventStream> wrap(final Callable<EventStream> callable) {
		return new Callable<EventStream>() {
			@Override
			public EventStream call() throws Exception {
				try(EventStream strm = callable.call()) {
					long previousBinNum = -1;
					SummaryStatistics stats = new SummaryStatistics();
					ArrayListEventStream ret = new ArrayListEventStream(0, (RemotableEventStreamDesc) strm.getDescription());
					ArrayListEventStream buf = new ArrayListEventStream(0, (RemotableEventStreamDesc) strm.getDescription());
					for(Event e : strm) {
						long epochSeconds = e.getEpochSeconds();
						long binNumber = epochSeconds/intervalSecs;
						if(binNumber != previousBinNum) {
							addStat(stats, buf, binNumber, ret);
							buf = new ArrayListEventStream(0, (RemotableEventStreamDesc) strm.getDescription());
							previousBinNum = binNumber;
						}
						buf.add(e.makeClone());
						double val = e.getSampleValue().getValue().doubleValue();
						if(!Double.isNaN(val)) { 
							stats.addValue(val);
						}
					}
					// Catch the last bin
					addStat(stats, buf, previousBinNum, ret);
					return ret;
				}
			}
	
			private void addStat(SummaryStatistics stats, ArrayListEventStream buf, long binNumber, ArrayListEventStream ret) {
				if(binNumber == -1)  return;
				if(stats.getN() <= 0) { 
					logger.debug("No values were added into the sumary stats");
					stats.clear();
					return;
				}
				
				double mean = stats.getMean();
				double cutoff = numDeviations*stats.getStandardDeviation();
				for(Event e : buf) {
					double val = e.getSampleValue().getValue().doubleValue();
					if(Math.abs(val - mean) <= cutoff) {
						ret.add(e);
					} else {
						// if(logger.isDebugEnabled()) logger.debug("Skipping value " + val + " based on mean=" + mean + " and stdz = " + stdz + " for timestamp " + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()));
					}
				}
				stats.clear();
			}
	
		};
	}

	@Override
	public String getIdentity() {
		return "ignoreflyers";
	}

	@Override
	public String getExtension() {
		if(intervalSecs == PostProcessors.DEFAULT_SUMMARIZING_INTERVAL && numDeviations == DEFAULT_DEVIATIONS) {
			return getIdentity();
		} else {
			return getIdentity() + "_" + Integer.toString(intervalSecs) + "_" + Float.toString(numDeviations);
		}
	}
}
