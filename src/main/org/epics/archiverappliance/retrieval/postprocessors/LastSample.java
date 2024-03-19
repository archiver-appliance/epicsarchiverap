package org.epics.archiverappliance.retrieval.postprocessors;

import edu.stanford.slac.archiverappliance.PB.data.PBParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.concurrent.Callable;

/**
 * Similar to the firstSample operator with the exception that we use the last sample in the bin.\
 * @author mshankar
 *
 */

// The implementation however is similar to lastFill.
public class LastSample implements PostProcessor, PostProcessorWithConsolidatedEventStream {
	private static Logger logger = LogManager.getLogger(LastSample.class.getName());
	private int intervalSecs = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;
	private long firstBin = 0;
	private long lastBin = Long.MAX_VALUE;
	private long currentBin = -1;
	private LinkedHashMap<Long, Event> bin2Event = new LinkedHashMap<Long, Event>(); 
	RemotableEventStreamDesc srcDesc = null;
	Event lastSampleBeforeStart = null;
	boolean lastSampleBeforeStartAdded = false;
	
	@Override
	public void initialize(String userarg, String pvName) throws IOException {
		if(userarg != null && userarg.contains("_")) {
			String[] userparams = userarg.split("_");
			String intervalStr = userparams[1];
			intervalSecs = Integer.parseInt(intervalStr);
			logger.debug("lastSample, using user supplied interval of " + intervalSecs);
		} else {
			logger.debug("lastSample, using the default interval of  " + intervalSecs + " as the user has not specified the interval argument.");
		}
	}

	@Override
    public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) {
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
					if(srcDesc == null) srcDesc = (RemotableEventStreamDesc) strm.getDescription();
					ArrayListEventStream buf = new ArrayListEventStream(0, (RemotableEventStreamDesc) strm.getDescription());
					for(Event e : strm) {
						try { 
							long epochSeconds = e.getEpochSeconds();
							long binNumber = epochSeconds/intervalSecs;
							if(binNumber >= firstBin && binNumber <= lastBin) { 
								if(!lastSampleBeforeStartAdded && lastSampleBeforeStart != null) { 
									logger.debug("Adding lastSampleBeforeStart to bin " + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEpochSeconds()));
									bin2Event.put(firstBin-1, lastSampleBeforeStart);
									lastSampleBeforeStartAdded = true; 
								}
								if(binNumber != currentBin) {
									currentBin = binNumber;
									bin2Event.put(currentBin, e.makeClone());		
								} else { 
									if(!bin2Event.containsKey(currentBin)) {
										bin2Event.put(currentBin, e.makeClone());		
									} else { 
										Event currentBinEvent = bin2Event.get(currentBin);
                                        if (e.getEventTimeStamp().isAfter(currentBinEvent.getEventTimeStamp())) {
											bin2Event.put(currentBin, e.makeClone());		
										}
									}
								}
							} else if(binNumber < firstBin) { 
								// Michael Davidsaver's special case; keep track of the last value before the start time and then add that in as a single sample.
								if(!lastSampleBeforeStartAdded) { 
									if(lastSampleBeforeStart != null) { 
										if(e.getEpochSeconds() >= lastSampleBeforeStart.getEpochSeconds()) { 
											lastSampleBeforeStart = e.makeClone();
										}
										logger.info("Setting the lastSampleBeforeStart to " + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEventTimeStamp()));
									} else { 
										lastSampleBeforeStart = e.makeClone();
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
		return "lastSample";
	}

	@Override
	public String getExtension() {
		if(intervalSecs == PostProcessors.DEFAULT_SUMMARIZING_INTERVAL) {
			return "lastSample";
		} else {
			return "lastSample_" + Integer.toString(intervalSecs);
		}
	}

	@Override
	public EventStream getConsolidatedEventStream() {
		if(!lastSampleBeforeStartAdded && lastSampleBeforeStart != null) { 
			bin2Event.put(firstBin-1, lastSampleBeforeStart);
			lastSampleBeforeStartAdded = true; 
			logger.debug("Adding lastSampleBeforeStart to bin " + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEpochSeconds()));
		}
		if(bin2Event.isEmpty()) { 
			return new ArrayListEventStream(0, srcDesc);
		} else { 
			return new FillsCollectorEventStream(this.firstBin == 0 ? 0 : this.firstBin-1, lastBin, intervalSecs, srcDesc, bin2Event, false);
		}
	}

	@Override
	public long getStartBinEpochSeconds() {
		return this.firstBin*this.intervalSecs;
	}

	@Override
	public long getEndBinEpochSeconds() {
		return this.lastBin*this.intervalSecs;
	}

	/* (non-Javadoc)
	 * @see org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream#getBinTimestamps()
	 */
	@Override
	public LinkedList<TimeSpan> getBinTimestamps() {
		return SummaryStatsPostProcessor.getBinTimestamps(this.firstBin, this.lastBin, this.intervalSecs);
	}
}
