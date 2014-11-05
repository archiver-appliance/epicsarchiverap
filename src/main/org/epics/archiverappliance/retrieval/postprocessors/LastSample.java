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

import edu.stanford.slac.archiverappliance.PB.data.PBParseException;

/**
 * Similar to the firstSample operator with the exception that we use the last sample in the bin.\
 * @author mshankar
 *
 */

// The implementation however is similar to lastFill.
public class LastSample implements PostProcessor, PostProcessorWithConsolidatedEventStream {
	private static Logger logger = Logger.getLogger(LastSample.class.getName());
	private int intervalSecs = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;
	private long firstBin = 0;
	private long lastBin = 0;
	private long currentBin = -1;
	private LinkedHashMap<Long, Event> bin2Event = new LinkedHashMap<Long, Event>(); 
	RemotableEventStreamDesc srcDesc = null;
	
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
						try { 
							long epochSeconds = e.getEpochSeconds();
							long binNumber = epochSeconds/intervalSecs;
							if(binNumber >= firstBin && binNumber <= lastBin) { 
								if(binNumber != currentBin) {
									currentBin = binNumber;
									bin2Event.put(currentBin, e.makeClone());		
								} else { 
									if(!bin2Event.containsKey(currentBin)) {
										bin2Event.put(currentBin, e.makeClone());		
									} else { 
										Event currentBinEvent = bin2Event.get(currentBin);
										if(e.getEventTimeStamp().after(currentBinEvent.getEventTimeStamp())) { 
											bin2Event.put(currentBin, e.makeClone());		
										}
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
		return new FillsCollectorEventStream(firstBin, lastBin, intervalSecs, srcDesc, bin2Event, false);
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
