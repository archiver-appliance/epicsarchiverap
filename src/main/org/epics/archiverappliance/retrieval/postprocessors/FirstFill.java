package org.epics.archiverappliance.retrieval.postprocessors;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.concurrent.Callable;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

/**
 * Similar to the firstSample operator with the exception that we alter the timestamp to the middle of the bin and copy over the previous bin's value if a bin does not have any samples.
 * @author mshankar
 *
 */
public class FirstFill implements PostProcessor, PostProcessorWithConsolidatedEventStream {
	private static Logger logger = Logger.getLogger(FirstFill.class.getName());
	private int intervalSecs = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;
	private long firstBin = 0;
	private long lastBin = Long.MAX_VALUE;
	private long currentBin = -1;
	private LinkedHashMap<Long, Event> bin2Event = new LinkedHashMap<Long, Event>(); 
	RemotableEventStreamDesc srcDesc = null;
	
	@Override
	public void initialize(String userarg, String pvName) throws IOException {
		if(userarg != null && userarg.contains("_")) {
			String[] userparams = userarg.split("_");
			String intervalStr = userparams[1];
			intervalSecs = Integer.parseInt(intervalStr);
			logger.debug("FirstFill, using user supplied interval of " + intervalSecs);
		} else {
			logger.debug("FirstFill, using the default interval of  " + intervalSecs + " as the user has not specified the interval argument.");
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
					if(srcDesc == null) srcDesc = (RemotableEventStreamDesc) strm.getDescription();
					ArrayListEventStream buf = new ArrayListEventStream(0, (RemotableEventStreamDesc) strm.getDescription());
					for(Event e : strm) {
						long epochSeconds = e.getEpochSeconds();
						long binNumber = epochSeconds/intervalSecs;
						if(binNumber >= firstBin && binNumber <= lastBin) { 
							if(binNumber != currentBin) {
								currentBin = binNumber;
								if(!bin2Event.containsKey(currentBin)) {
									bin2Event.put(currentBin, e.makeClone());		
								} else { 
									Event currentBinEvent = bin2Event.get(currentBin);
									if(currentBin == firstBin) { 
										// If this is the first bin, we replace the existing value only if event is from a previous bin before start time
										// See note below
										long existingEventsBin = currentBinEvent.getEpochSeconds()/intervalSecs;
										if(existingEventsBin < firstBin) {
											Event clonedEvent = e.makeClone();
											bin2Event.put(currentBin, clonedEvent);
											currentBinEvent = clonedEvent;
										}
									} 
									if(e.getEventTimeStamp().before(currentBinEvent.getEventTimeStamp())) { 
										bin2Event.put(currentBin, e.makeClone());		
									}
								}
							}
						} else if(binNumber < firstBin) {
							// We prefer the first sample in the bin. However, in case that bin is empty, we can use samples from before the start time
							// See note above.
							bin2Event.put(firstBin, e.makeClone());	
						}
					}
					return buf;
				}
			}

		};
	}
	
	@Override
	public String getIdentity() {
		return "firstFill";
	}

	@Override
	public String getExtension() {
		if(intervalSecs == PostProcessors.DEFAULT_SUMMARIZING_INTERVAL) {
			return "firstFill";
		} else {
			return "firstFill_" + Integer.toString(intervalSecs);
		}
	}

	@Override
	public EventStream getConsolidatedEventStream() {
		return new FillsCollectorEventStream(firstBin, lastBin, intervalSecs, srcDesc, bin2Event);
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
