package org.epics.archiverappliance.retrieval.postprocessors;

import edu.stanford.slac.archiverappliance.PB.data.PBParseException;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.concurrent.Callable;

/**
 * Approx implementation of ChannelArchiver plotbinning for Michael Davidsaver
 * From the doc
 * <ol>
 * <li>If there is no sample for the time span of a bin, the bin remains empty.</li>
 * <li>If there is one sample, it is placed in the bin.</li>
 * <li>If there are two samples, they are placed in the bin</li>
 * <li> If there are more than two samples, the first and last one are placed in the bin. 
 * In addition, two artificial samples are created with a time stamp right
 * between the first and last sample. One contains the minimum, the other
 * the maximum of all raw samples who's time stamps fall into the bin. They
 * are presented to the user in the sequence initial, minimum, maximum, final.</li>
 * </ol>
 *  
 * @author mshankar
 *
 */
public class CAPlotBinning implements PostProcessor, PostProcessorWithConsolidatedEventStream {
	@Override
	public String getIdentity() { 
		return "caplotbinning";
	}

	enum SampleLocation { 
		ASIS, 
		FIRSTSAMPLE, 
		MINSAMPLE, 
		MAXSAMPLE, 
		LASTSAMPLE;
		
		long getLocationEpochSeconds(long eventEpochSeconds, long binNumber, int intervalSecs) { 
			switch(this) { 
			case ASIS:
				return eventEpochSeconds;
			case FIRSTSAMPLE:
				return binNumber*intervalSecs;
			case MINSAMPLE:
				return (binNumber*intervalSecs + ((int) (0.25*intervalSecs)));
			case MAXSAMPLE:
				return (binNumber*intervalSecs + ((int) (0.50*intervalSecs)));
			case LASTSAMPLE:
				return (binNumber*intervalSecs + ((int) (0.75*intervalSecs)));
			default:
				return 0;
			}
		}
	} 
	
	private static Logger logger = LogManager.getLogger(CAPlotBinning.class.getName());
	int intervalSecs = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;
	
    private Instant previousEventTimestamp = Instant.ofEpochMilli(1);
	
	LinkedHashMap<Long, PlotBin> consolidatedData = new LinkedHashMap<Long, PlotBin>();
	
	long firstBin = 0;
	long lastBin = Long.MAX_VALUE;
	long currentBin = -1;
	RemotableEventStreamDesc srcDesc = null;

	Event lastSampleBeforeStart = null;
	boolean lastSampleBeforeStartAdded = false;

	@Override
    public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) {
		firstBin = TimeUtils.convertToEpochSeconds(start)/intervalSecs;
		lastBin = TimeUtils.convertToEpochSeconds(end)/intervalSecs;
		logger.debug("Expecting " + lastBin + " - " + firstBin + " values " + (lastBin+2 - firstBin)); // Add 2 for the first and last bins..
		float storageRate = typeInfo.getComputedStorageRate();
		long numSeconds = TimeUtils.convertToEpochSeconds(end) - TimeUtils.convertToEpochSeconds(start);
		// Add a fudge factor of 2 for java
		long estimatedMemoryConsumption = (long) (storageRate*4*numSeconds*2/intervalSecs);
		return estimatedMemoryConsumption;
	}
	private PlotBin currentPlotBin = null;
	
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
	public Callable<EventStream> wrap(final Callable<EventStream> callable) {
		return new Callable<EventStream>() {
			@Override
			public EventStream call() throws Exception {
				try(EventStream strm = callable.call()) {
					// If we cache the mean/sigma etc, then we should add something to the desc telling us that this is cached data and then we can replace the stat value for that bin?
					RemotableEventStreamDesc strmDesc = (RemotableEventStreamDesc) strm.getDescription();
					if(srcDesc == null) srcDesc = new RemotableEventStreamDesc(ArchDBRTypes.DBR_SCALAR_DOUBLE, strmDesc.getPvName(), strmDesc.getYear());
					for(Event e : strm) {
						try {
							DBRTimeEvent dbrTimeEvent = (DBRTimeEvent) e;
							long epochSeconds = dbrTimeEvent.getEpochSeconds();
                            if (dbrTimeEvent.getEventTimeStamp().isAfter(previousEventTimestamp)) {
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
								if(!lastSampleBeforeStartAdded && lastSampleBeforeStart != null) {
									switchToNewBin(firstBin-1);
									currentPlotBin.addEvent(lastSampleBeforeStart);
									lastSampleBeforeStartAdded = true;
								}

								if(binNumber != currentBin) {
									switchToNewBin(binNumber);
								}

								currentPlotBin.addEvent(e);

								if(dbrTimeEvent.getSeverity() > currentPlotBin.maximizedSeverity) {
									currentPlotBin.maximizedSeverity = dbrTimeEvent.getSeverity();
								}

								if(dbrTimeEvent.hasFieldValues() && dbrTimeEvent.getFields().containsKey("cnxregainedepsecs")) {
									currentPlotBin.connectionChanged= true;
								}

							} else if(binNumber < firstBin) {
								if(!lastSampleBeforeStartAdded) {
									if(lastSampleBeforeStart != null) {
										if(e.getEpochSeconds() >= lastSampleBeforeStart.getEpochSeconds()) {
											lastSampleBeforeStart = e.makeClone();
										}
									} else {
										lastSampleBeforeStart = e.makeClone();
									}
								}
							}
						} catch(PBParseException ex) {
							logger.error("Skipping possible corrupted event for pv " + strmDesc);
						}
					}
					return getConsolidatedEventStream();
				}
			}

			private void switchToNewBin(long binNumber) {
				currentBin = binNumber;
				currentPlotBin = new PlotBin(currentBin);
				consolidatedData.put(currentBin, currentPlotBin);
			}
		};
	}

	class PlotBin {
		Event firstSample;
		Event lastSample;
		SummaryStatistics stats = new SummaryStatistics();
		long binNumber = 0;
		/**
		 * Maximize severity
		 */
		int maximizedSeverity = 0;
		/**
		 * Do we have any connection changed events
		 */
		boolean connectionChanged = false;


		PlotBin(long binNumber) {
			this.binNumber = binNumber;
		}

		void addEvent(Event e) {
			if(firstSample == null) {
				firstSample = e.makeClone();
				stats.addValue(firstSample.getSampleValue().getValue().doubleValue());
				return;
			}
			if(lastSample == null) {
				lastSample = e.makeClone();
				stats.addValue(lastSample.getSampleValue().getValue().doubleValue());
				return;

			}
			// We have more than two samples
            if (e.getEventTimeStamp().isAfter(lastSample.getEventTimeStamp())) {
				lastSample = e.makeClone();
				stats.addValue(lastSample.getSampleValue().getValue().doubleValue());
				return;
			}
		}


		public void outputEvents(ArrayListEventStream ret) {
			// If there is no sample for the time span of a bin, the bin remains empty
			if(firstSample == null) {
				return;
			}

			if(ret.getDescription().getYear() == -1) {
				short curYear = TimeUtils.computeYearForEpochSeconds(firstSample.getEpochSeconds());
				logger.debug("Initialize the current year as the year of the first bin with a value it it " + curYear);
				ret.getDescription().setYear(curYear);
			}

			// If there is one sample, it is placed in the bin
			if(lastSample == null) {
				ret.add(makeEvent(SampleLocation.ASIS, firstSample.getEpochSeconds(), firstSample.getSampleValue().getValue().doubleValue()));
				return;
			}
			// If there are two samples, they are placed in the bin
			if(stats.getN() == 2) {
				ret.add(makeEvent(SampleLocation.ASIS, firstSample.getEpochSeconds(), firstSample.getSampleValue().getValue().doubleValue()));
				ret.add(makeEvent(SampleLocation.ASIS, lastSample.getEpochSeconds(), lastSample.getSampleValue().getValue().doubleValue()));
				return;
			}

			// If there are more than two samples...
			// presented to the user in the sequence initial, minimum, maximum, final
			long binStartEpochSeconds = binNumber*intervalSecs;
			ret.add(makeEvent(SampleLocation.FIRSTSAMPLE, binStartEpochSeconds, firstSample.getSampleValue().getValue().doubleValue()));
			ret.add(makeEvent(SampleLocation.MINSAMPLE, binStartEpochSeconds, stats.getMin()));
			ret.add(makeEvent(SampleLocation.MAXSAMPLE, binStartEpochSeconds, stats.getMax()));
			ret.add(makeEvent(SampleLocation.LASTSAMPLE, binStartEpochSeconds, lastSample.getSampleValue().getValue().doubleValue()));
		}


		private Event makeEvent(SampleLocation location, long binStartEpochSeconds, double value) {
			long epochSeconds = location.getLocationEpochSeconds(binStartEpochSeconds, binNumber, intervalSecs);
			// logger.info("Location " + location.toString() + " epochSeconds " + epochSeconds + " for bin " + binNumber + " and intervalSecs " + intervalSecs);
			POJOEvent pojoEvent = new POJOEvent(ArchDBRTypes.DBR_SCALAR_DOUBLE,
					TimeUtils.convertFromEpochSeconds(epochSeconds, 0),
					new ScalarValue<Double>(value),
					0, this.maximizedSeverity);
			DBRTimeEvent pbevent = (DBRTimeEvent) pojoEvent.makeClone();
			if(this.connectionChanged) {
				pbevent.addFieldValue("connectionChange", "true");
			}
			return pbevent;
		}
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
		if(consolidatedData.isEmpty()) { 
			return new ArrayListEventStream(0, this.srcDesc);			
		} else { 
			if(lastBin == Long.MAX_VALUE) { 
				this.firstBin = Collections.min(consolidatedData.keySet());
				this.lastBin = Collections.max(consolidatedData.keySet());
			}
			ArrayListEventStream ret = new ArrayListEventStream((int) (lastBin - firstBin + 1), this.srcDesc);
			for(long curBin = this.firstBin-1; curBin <= this.lastBin; curBin++) { 
				if(consolidatedData.containsKey(curBin)) { 
					consolidatedData.get(curBin).outputEvents(ret);
				}
			}
			
			return new ArrayListCollectorEventStream(ret);
		}

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
	
}