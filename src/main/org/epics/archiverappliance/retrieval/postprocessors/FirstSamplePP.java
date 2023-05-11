package org.epics.archiverappliance.retrieval.postprocessors;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletRequest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
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
public class FirstSamplePP implements PostProcessor, AfterAllStreams {
	private static Logger logger = LogManager.getLogger(FirstSamplePP.class.getName());
	private int intervalSecs = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;
	private long firstBin = 0;
	private long lastBin = Long.MAX_VALUE;
	private long previousBinNum = -1;
	Event lastSampleBeforeStart = null;
	boolean lastSampleBeforeStartAdded = false;
	private EventStreamDesc lastSampleDesc;

	
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
									if(!lastSampleBeforeStartAdded && lastSampleBeforeStart != null) { 
										logger.info("Adding the lastSampleBeforeStart at " + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEventTimeStamp()) + " into the result stream");
										buf.add(lastSampleBeforeStart);
										lastSampleBeforeStartAdded = true; 
										lastSampleDesc = null;
									}
									buf.add(e.makeClone());
									previousBinNum = binNumber;
									logger.debug("Bin Number " + binNumber + " First: " + firstBin + " Last: " + lastBin);
								}
							} else if(binNumber < firstBin) { 
								// Michael Davidsaver's special case; keep track of the last value before the start time and then add that in as a single sample.
								if(!lastSampleBeforeStartAdded) { 
									if(lastSampleBeforeStart != null) { 
										if(e.getEpochSeconds() >= lastSampleBeforeStart.getEpochSeconds()) { 
											lastSampleBeforeStart = e.makeClone();
											lastSampleDesc = strm.getDescription();
											logger.info("Resetting the lastSampleBeforeStart to " + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEventTimeStamp()));
										}
									} else { 
										lastSampleBeforeStart = e.makeClone();
										lastSampleDesc = strm.getDescription();
										logger.info("Setting the lastSampleBeforeStart to " + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEventTimeStamp()));
									}
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

	@Override
	public EventStream anyFinalData() {
		if(!lastSampleBeforeStartAdded && lastSampleBeforeStart != null) { 
			ArrayListEventStream buf = new ArrayListEventStream(0, (RemotableEventStreamDesc) lastSampleDesc);
			logger.info("Returning the lastSampleBeforeStart at " + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEventTimeStamp()) + " into the result stream after all the other streams have been processed.");
			buf.add(lastSampleBeforeStart);
			lastSampleBeforeStartAdded = true; 
			lastSampleDesc = null;
			return buf;
		}
		return null;
	}
}
