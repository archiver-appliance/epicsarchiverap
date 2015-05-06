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
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

/**
 * Abstract class for various operators that operate on a SummaryStatistics
 * Child classes implement the getIdentity and the getStats method.
 * @author mshankar
 *
 */
public abstract class SummaryStatsPostProcessor implements PostProcessor, PostProcessorWithConsolidatedEventStream, FillNoFillSupport {
	@Override
	public abstract String getIdentity();
	public abstract SummaryStatsCollector getCollector();
	
	private static Logger logger = Logger.getLogger(SummaryStatsPostProcessor.class.getName());
	int intervalSecs = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;
	private Timestamp previousEventTimestamp = new Timestamp(1);
	
	class SummaryValue { 
		/**
		 * The summary value
		 */
		double value;
		/**
		 * Maximize severity
		 */
		int severity;
		/**
		 * Do we have any connection changed events
		 */
		boolean connectionChanged;

		public SummaryValue(double value, int severity, boolean connectionChanged) {
			this.value = value;
			this.severity = severity;
			this.connectionChanged = connectionChanged;
		}
		
		
	}

	protected LinkedHashMap<Long, SummaryValue> consolidatedData = new LinkedHashMap<Long, SummaryValue>();
	long firstBin = 0;
	long lastBin = 0;
	long currentBin = -1;
	int currentMaxSeverity = 0;
	boolean currentConnectionChangedEvents = false;
	SummaryStatsCollector currentBinCollector = null;
	RemotableEventStreamDesc srcDesc = null;
	private boolean inheritValuesFromPreviousBins = true;
	
	@Override
	public void initialize(String userarg, String pvName) throws IOException {
		if(userarg != null && userarg.contains("_")) {
			String[] userparams = userarg.split("_");
			String intervalStr = userparams[1];
			intervalSecs = Integer.parseInt(intervalStr);
			logger.debug("Using use supplied interval of " + intervalSecs);
		} else {
			logger.debug("Using the default interval of  " + intervalSecs + " as the user has not specified the interval argument.");
		}
	}
	
	

	@Override
	public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Timestamp start, Timestamp end, HttpServletRequest req) {
		firstBin = TimeUtils.convertToEpochSeconds(start)/intervalSecs;
		lastBin = TimeUtils.convertToEpochSeconds(end)/intervalSecs;
		logger.debug("Expecting " + lastBin + " - " + firstBin + " values " + (lastBin+2 - firstBin)); // Add 2 for the first and last bins..
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
					// If we cache the mean/sigma etc, then we should add something to the desc telling us that this is cached data and then we can replace the stat value for that bin?
					if(srcDesc == null) srcDesc = (RemotableEventStreamDesc) strm.getDescription();
					for(Event e : strm) {
						DBRTimeEvent dbrTimeEvent = (DBRTimeEvent) e;
						long epochSeconds = dbrTimeEvent.getEpochSeconds();
						if(dbrTimeEvent.getEventTimeStamp().after(previousEventTimestamp)) { 
							previousEventTimestamp = dbrTimeEvent.getEventTimeStamp();
						} else {
							// Note that this is expected. ETL is not transactional; so we can get the same event twice from different stores.
							if(logger.isDebugEnabled()) { 
								logger.debug("Skipping older event " + TimeUtils.convertToHumanReadableString(dbrTimeEvent.getEventTimeStamp()) + " previous " + TimeUtils.convertToHumanReadableString(previousEventTimestamp));
							}
							continue;
						}
						long binNumber = epochSeconds/intervalSecs;
						if(binNumber >= firstBin && binNumber <= lastBin) {
							// We only add bins for the specified time frame. 
							// The ArchiveViewer depends on the number of values being the same and because of different rates for PVs, the bin number for the starting bin could be different...
							// We could add a firstbin-1 and put all values before the starting timestamp in that bin but that would give incorrect summaries.
							if(binNumber != currentBin) {
								if(currentBin != -1) { 
									consolidatedData.put(currentBin, new SummaryValue(currentBinCollector.getStat(), currentMaxSeverity, currentConnectionChangedEvents));
								}
								switchToNewBin(binNumber);
							}
							currentBinCollector.addEvent(e);
							if(dbrTimeEvent.getSeverity() > currentMaxSeverity) { 
								currentMaxSeverity = dbrTimeEvent.getSeverity();
							}
							if(dbrTimeEvent.hasFieldValues() && dbrTimeEvent.getFields().containsKey("cnxregainedepsecs")) { 
								currentConnectionChangedEvents = true;
							}
						} else { 
							logger.debug("Skipping event in bin outside range " + binNumber);
						}
					}
					return new SummaryStatsCollectorEventStream(firstBin, lastBin, intervalSecs, srcDesc, consolidatedData, inheritValuesFromPreviousBins);
				}
			}

			private void switchToNewBin(long binNumber) {
				currentBin = binNumber;
				currentMaxSeverity = 0;
				currentConnectionChangedEvents = false;
				currentBinCollector = getCollector();
				currentBinCollector.setBinParams(intervalSecs, currentBin);
			}
		};
	}


	@Override
	public String getExtension() {
		String identity = this.getIdentity();
		if(intervalSecs == PostProcessors.DEFAULT_SUMMARIZING_INTERVAL) {
			return identity;
		} else {
			return identity + "_" + Integer.toString(intervalSecs);
		}
	}
	
	
	@Override
	public EventStream getConsolidatedEventStream() {
		if(currentBin != -1) { 
			consolidatedData.put(currentBin, new SummaryValue(currentBinCollector.getStat(), currentMaxSeverity, currentConnectionChangedEvents));
			currentBinCollector = null;
		}

		return new SummaryStatsCollectorEventStream(firstBin, lastBin, intervalSecs, srcDesc, consolidatedData, inheritValuesFromPreviousBins);
	}
	/* (non-Javadoc)
	 * @see org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream#getStartBinEpochSeconds()
	 */
	@Override
	public long getStartBinEpochSeconds() {
		return this.firstBin*this.intervalSecs;
	}
	/* (non-Javadoc)
	 * @see org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream#getEndBinEpochSeconds()
	 */
	@Override
	public long getEndBinEpochSeconds() {
		return this.lastBin*this.intervalSecs;
	}
	/* (non-Javadoc)
	 * @see org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream#getBinTimestamps()
	 */
	@Override
	public LinkedList<TimeSpan> getBinTimestamps() {
		return getBinTimestamps(this.firstBin, this.lastBin, this.intervalSecs);
	}
	
	public static LinkedList<TimeSpan> 	getBinTimestamps(long firstBin, long lastBin, int intervalSecs) { 
		LinkedList<TimeSpan> ret = new LinkedList<TimeSpan>();
		long previousBinEpochSeconds = firstBin*intervalSecs;
		for(long currentBin = firstBin+1; currentBin <= lastBin; currentBin++) { 
			long currentBinEpochSeconds = currentBin*intervalSecs;
			ret.add(new TimeSpan(previousBinEpochSeconds, currentBinEpochSeconds));
			previousBinEpochSeconds = currentBinEpochSeconds;
		}
		return ret;
	}

	@Override
	public void doNotInheritValuesFromPrevioisBins() {
		this.inheritValuesFromPreviousBins = false;
	}
}