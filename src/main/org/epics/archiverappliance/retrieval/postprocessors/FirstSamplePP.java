package org.epics.archiverappliance.retrieval.postprocessors;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

import edu.stanford.slac.archiverappliance.PB.data.PBParseException;

/**
 * This takes "interval" argument and returns only the first sample in that interval...
 * @author mshankar
 *
 */
public class FirstSamplePP implements PostProcessor {
	private static Logger logger = Logger.getLogger(FirstSamplePP.class.getName());
	private int intervalSecs = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;
	private long firstBin = 0;
	private long lastBin = Long.MAX_VALUE;
	private long previousBinNum = -1;

	
	@Override
	public void initialize(String userarg, String pvName) throws IOException {
		if(userarg != null && userarg.contains("_")) {
			String[] userparams = userarg.split("_");
			String intervalStr = userparams[1];
			intervalSecs = Integer.parseInt(intervalStr);
			logger.debug("FirstSamplePP, using user supplied interval of " + intervalSecs);
		} else {
			logger.debug("FirstSamplePP, using the default interval of  " + intervalSecs + " as the user has not specified the interval argument.");
		}
	}

	@Override
	public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Timestamp start, Timestamp end, HttpServletRequest req) {
		firstBin = TimeUtils.convertToEpochSeconds(start)/intervalSecs;
		lastBin = TimeUtils.convertToEpochSeconds(end)/intervalSecs;
		float storageRate = typeInfo.getComputedStorageRate();
		long numSeconds = TimeUtils.convertToEpochSeconds(end) - TimeUtils.convertToEpochSeconds(start);
		// Add a fudge factor of 2 for java 
		long estimatedMemoryConsumption = (long) (storageRate*numSeconds*2/intervalSecs);
		return estimatedMemoryConsumption;
	}

	@Override
	public Callable<EventStream> wrap(final Callable<EventStream> callable) {
		return new Callable<EventStream>() {
			@Override
			public EventStream call() throws Exception {
				try(EventStream strm = callable.call()) {
					ArrayListEventStream buf = new ArrayListEventStream(0, (RemotableEventStreamDesc) strm.getDescription());
					for(Event e : strm) {
						try { 
							long epochSeconds = e.getEpochSeconds();
							long binNumber = epochSeconds/intervalSecs;
							if(binNumber >= firstBin && binNumber <= lastBin) {
								if(binNumber != previousBinNum) {
									buf.add(e.makeClone());
									previousBinNum = binNumber;
									logger.debug("Bin Number " + binNumber + " First: " + firstBin + " Last: " + lastBin);
								}
							}
						} catch(PBParseException ex) { 
							logger.error("Skipping possible corrupted event for pv " + strm.getDescription());
						}
					}
					return buf;
				}
			}

		};
	}

	@Override
	public String getIdentity() {
		return "firstSample";
	}

	@Override
	public String getExtension() {
		if(intervalSecs == PostProcessors.DEFAULT_SUMMARIZING_INTERVAL) {
			return "firstSample";
		} else {
			return "firstSample_" + Integer.toString(intervalSecs);
		}
	}
}
