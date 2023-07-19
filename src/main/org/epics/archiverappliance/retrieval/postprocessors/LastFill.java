package org.epics.archiverappliance.retrieval.postprocessors;

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
 * Similar to the firstFill operator with the exception that we use the last sample in the bin.
 * @author mshankar
 *
 */
public class LastFill implements PostProcessor, PostProcessorWithConsolidatedEventStream {
	private static Logger logger = LogManager.getLogger(LastFill.class.getName());
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
			logger.debug("LastFill, using user supplied interval of " + intervalSecs);
		} else {
			logger.debug("LastFill, using the default interval of  " + intervalSecs + " as the user has not specified the interval argument.");
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
							if(!lastSampleBeforeStartAdded) { 
								if(lastSampleBeforeStart != null) { 
									if(e.getEpochSeconds() >= lastSampleBeforeStart.getEpochSeconds()) { 
										lastSampleBeforeStart = e.makeClone();
										logger.debug("Overriding lastSampleBeforeStart " + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEpochSeconds()));
									} else { 
										logger.debug("Skipping as current event is before lastSampleBeforeStart " + TimeUtils.convertToHumanReadableString(e.getEpochSeconds()));
									}
								} else { 
									lastSampleBeforeStart = e.makeClone();
									logger.debug("Adding lastSampleBeforeStart" + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEpochSeconds()));
								}
							}
						}
					}
					return buf;
				}
			}

		};
	}
	
	@Override
	public String getIdentity() {
		return "lastFill";
	}

	@Override
	public String getExtension() {
		if(intervalSecs == PostProcessors.DEFAULT_SUMMARIZING_INTERVAL) {
			return "lastFill";
		} else {
			return "lastFill_" + Integer.toString(intervalSecs);
		}
	}

	@Override
	public EventStream getConsolidatedEventStream() {
		if(!lastSampleBeforeStartAdded && lastSampleBeforeStart != null) { 
			logger.debug("Adding lastSampleBeforeStart to bin " + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEpochSeconds()));
			bin2Event.put(firstBin-1, lastSampleBeforeStart);
			lastSampleBeforeStartAdded = true; 
		}
		if(bin2Event.isEmpty()) { 
			return new ArrayListEventStream(0, srcDesc);
		} else { 
			return new FillsCollectorEventStream(this.firstBin == 0 ? 0 : this.firstBin-1, lastBin, intervalSecs, srcDesc, bin2Event);
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
