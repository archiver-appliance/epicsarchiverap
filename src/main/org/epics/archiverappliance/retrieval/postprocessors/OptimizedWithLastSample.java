package org.epics.archiverappliance.retrieval.postprocessors;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.data.PBParseException;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.postprocessors.SummaryStatsPostProcessor.SummaryValue;

import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;


/**
 * 
 * <code>OptimizedWithLastSample</code> expects one parameter at initialization, which is the number of requested points.
 * If there are less samples in the time interval than requested (with a certain deadband), all samples 
 * will be returned. If there are more samples than requested, the samples will be collected into bins.
 * Mean, std, min, max and count of each bin is calculated and returned as a single sample. 
 *
 * This differs from the <code>Optimized</code> post processor in that if a bin is empty, instead of
 * repeating the last bin with samples, the bin uses the last value of the last recorded sample instead
 * (as mean, min and max; stddev is zero and number of samples is also zero).
 */
public class OptimizedWithLastSample implements PostProcessor, PostProcessorWithConsolidatedEventStream, FillNoFillSupport {

    private static final Logger Logger = LogManager.getLogger(OptimizedWithLastSample.class.getName());
    private static final int DEFAULT_NUMBER_OF_POINTS = 1000;
    private static final String IDENTITY = "optimLastSample";
    
    private int numEvents;
    private ArrayListEventStream allEvents;
    private ArrayListEventStream transformedRawEvents;
    private int numberOfPoints = DEFAULT_NUMBER_OF_POINTS;
    private double lastValue = 0;

    private Instant previousEventTimestamp = Instant.ofEpochMilli(1);
    private int intervalSecs = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;
    protected LinkedHashMap<Long, SummaryValue> consolidatedData = new LinkedHashMap<Long, SummaryValue>();
    long firstBin = 0;
    long lastBin = Long.MAX_VALUE;
    long currentBin = -1;
    int currentMaxSeverity = 0;
    boolean currentConnectionChangedEvents = false;
    SummaryStatsCollector currentBinCollector = null;
    RemotableEventStreamDesc srcDesc = null;
    private boolean inheritValuesFromPreviousBins = true;
    Event lastSampleBeforeStart = null;
    boolean lastSampleBeforeStartAdded = false;
    
    Long lastBinSaved = null;
    
    
    private final Statistics statisticsPostProcessor = new Statistics(){
        @Override
        public SummaryStatsVectorCollector getCollector() {
            return new SummaryStatsVectorCollector() {
                private SummaryStatistics stats = new SummaryStatistics();
                @Override
                public void setBinParams(int intervalSecs, long binNum) {
                }
                
                @Override
                public boolean haveEventsBeenAdded() {
                    return stats.getN() > 0;
                }
                
                @Override
                public List<Double> getVectorValues() {
                    List<Double> list = new ArrayList<>(6);
                    list.add(stats.getMean());
                    list.add(stats.getStandardDeviation());
                    list.add(stats.getMin());
                    list.add(stats.getMax());
                    list.add((double)stats.getN());
                    list.add(lastValue);
                    return list;
                }
                
                @Override
                public double getStat() {
                    return Double.NaN;
                }
                            
                @Override
                public void addEvent(Event e) {
                 
                    if (numEvents < numberOfPoints) {
                        allEvents.add(e);
                    }
                    numEvents++;
                    double val = e.getSampleValue().getValue().doubleValue();
                    if(!Double.isNaN(val)) { 
                        lastValue = val;
                        stats.addValue(val);
                    } else { 
                        Logger.warn("Skipping NAN");
                    }
                }
            };
        }   
    };    
        
    @Override
    public void initialize(String userarg, String pvName) throws IOException {
        if(userarg != null && userarg.contains("_")) {
            String[] userparams = userarg.split("_");
            String numberStr = userparams[1];
            numberOfPoints = Integer.parseInt(numberStr);
        } 
        numEvents = 0;
    }
    
    @Override
    public long estimateMemoryConsumption(String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) {

        intervalSecs = (int) ((end.toEpochMilli() - start.toEpochMilli()) / (1000 * numberOfPoints));
        
        firstBin = TimeUtils.convertToEpochSeconds(start)/intervalSecs;
        lastBin = TimeUtils.convertToEpochSeconds(end)/intervalSecs;
        Logger.debug("Expecting " + lastBin + " - " + firstBin + " values " + (lastBin+2 - firstBin)); // Add 2 for the first and last bins..
        float storageRate = typeInfo.getComputedStorageRate();
        long numSeconds = TimeUtils.convertToEpochSeconds(end) - TimeUtils.convertToEpochSeconds(start);
        
        // Add a fudge factor of 2 for java 
        long estimatedMemoryConsumption = (long) (storageRate*numSeconds*2/intervalSecs);
        return estimatedMemoryConsumption;
        
    }
    
    @Override
    public void doNotInheritValuesFromPrevioisBins() {
        statisticsPostProcessor.doNotInheritValuesFromPrevioisBins();
    }

    @Override
    public boolean zeroOutEmptyBins() {
        return statisticsPostProcessor.zeroOutEmptyBins();
    }

    @Override
    public EventStream getConsolidatedEventStream() {
        if (numEvents > allEvents.size()) {
            return customStatsConsolidatedEventStream();
        } else {
            return new ArrayListCollectorEventStream(transformedRawEvents);
        }
    }
    
    private EventStream customStatsConsolidatedEventStream() {
      if(!lastSampleBeforeStartAdded && lastSampleBeforeStart != null) { 
        switchToNewBin(firstBin-1);
        Logger.debug("Adding lastSampleBeforeStart to bin " + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEpochSeconds()));
        currentBinCollector.addEvent(lastSampleBeforeStart);
        lastSampleBeforeStartAdded = true; 
      }
      if (currentBin != -1) {
        SummaryValue summaryValue;

        summaryValue = new SummaryValue(((SummaryStatsVectorCollector) currentBinCollector).getVectorValues(),
            currentMaxSeverity, currentConnectionChangedEvents);

        consolidatedData.put(currentBin, summaryValue);
        currentBinCollector = null;
      }
      if(consolidatedData.isEmpty()) { 
        return new ArrayListEventStream(0, srcDesc);      
      } else { 
        return new SummaryStatsCollectorEventStream(this.firstBin == 0 ? 0 : this.firstBin-1, this.lastBin, this.intervalSecs, srcDesc, consolidatedData, inheritValuesFromPreviousBins, zeroOutEmptyBins(), true, 6);
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

    @Override
    public LinkedList<TimeSpan> getBinTimestamps() {
      return SummaryStatsPostProcessor.getBinTimestamps(this.firstBin, this.lastBin, this.intervalSecs);
    }

    @Override
    public String getIdentity() {
        return IDENTITY;
    }

    @Override
    public String getExtension() {
        if(numberOfPoints == DEFAULT_NUMBER_OF_POINTS) {
            return getIdentity();
        } else {
            return getIdentity() + "_" + Integer.toString(numberOfPoints);
        }
    }

    @Override
    public Callable<EventStream> wrap(final Callable<EventStream> callable) {
        return new Callable<EventStream>() {
            public EventStream call() throws Exception {
                if (allEvents == null) {
                    EventStream strm = callable.call();
                    RemotableEventStreamDesc org = (RemotableEventStreamDesc)strm.getDescription();
                    RemotableEventStreamDesc desc = new RemotableEventStreamDesc(org);
                    allEvents = new ArrayListEventStream(numberOfPoints, desc);
                }
                Callable<EventStream> stCall = customStatsWrap(callable);
                EventStream stream = stCall.call();
                if (numEvents > allEvents.size()) {
                    return stream;
                } else {
                    transformedRawEvents = new ArrayListEventStream(allEvents.size(),allEvents.getDescription());
                    for (Event e : allEvents) {
                        transformedRawEvents.add(DBR2PBTypeMapping.getPBClassFor(e.getDBRType()).getSerializingConstructor().newInstance(e));
                    }
                    return transformedRawEvents;
                }
            }
        };
    }  
    
    public Callable<EventStream> customStatsWrap(final Callable<EventStream> callable) {
      return new Callable<EventStream>() {
        @Override
        public EventStream call() throws Exception {
          try (EventStream strm = callable.call()) {
            // If we cache the mean/sigma etc, then we should add something to
            // the desc telling us that this is cached data and then we can
            // replace the stat value for that bin?
            if (srcDesc == null)
              srcDesc = (RemotableEventStreamDesc) strm.getDescription();
            for (Event e : strm) {
              try {
                DBRTimeEvent dbrTimeEvent = (DBRTimeEvent) e;
                long epochSeconds = dbrTimeEvent.getEpochSeconds();
                  if (dbrTimeEvent.getEventTimeStamp().isAfter(previousEventTimestamp)) {
                  previousEventTimestamp = dbrTimeEvent.getEventTimeStamp();
                } else {
                  // Note that this is expected. ETL is not transactional; so we
                  // can get the same event twice from different stores.
                  if (Logger.isDebugEnabled()) {
                    Logger.debug("Skipping older event "
                        + TimeUtils.convertToHumanReadableString(dbrTimeEvent.getEventTimeStamp()) + " previous "
                        + TimeUtils.convertToHumanReadableString(previousEventTimestamp));
                  }
                  continue;
                }
                long binNumber = epochSeconds / intervalSecs;
                if (binNumber >= firstBin && binNumber <= lastBin) {
                  // We only add bins for the specified time frame.
                  // The ArchiveViewer depends on the number of values being the
                  // same and because of different rates for PVs, the bin number
                  // for the starting bin could be different...
                  // We could add a firstbin-1 and put all values before the
                  // starting timestamp in that bin but that would give
                  // incorrect summaries.
                  if (!lastSampleBeforeStartAdded && lastSampleBeforeStart != null) {
                    switchToNewBin(firstBin - 1);
                    currentBinCollector.addEvent(lastSampleBeforeStart);
                    lastSampleBeforeStartAdded = true;
                  }
                  if (binNumber != currentBin) {
                    if (currentBin != -1) {
                      if (lastBinSaved != null) {
                        long nSkip = currentBin-lastBinSaved;
                        if (nSkip > 1) {
                            // Bins have been skipped (had no events) so need to populate them
                            // with the last value from the last bin to have events
                            populateSkippedBins(nSkip);
                        }
                      }
                      
                      // We have the next set of events so evaluate current bin before moving on 
                      SummaryValue summaryValue;
                      summaryValue = new SummaryValue(
                          ((SummaryStatsVectorCollector) currentBinCollector).getVectorValues(), currentMaxSeverity,
                          currentConnectionChangedEvents);
                      consolidatedData.put(currentBin, summaryValue);
                      lastBinSaved = currentBin;
                    }
                    switchToNewBin(binNumber);
                  }
                  currentBinCollector.addEvent(e);
                  if (dbrTimeEvent.getSeverity() > currentMaxSeverity) {
                    currentMaxSeverity = dbrTimeEvent.getSeverity();
                  }
                  if (dbrTimeEvent.hasFieldValues() && dbrTimeEvent.getFields().containsKey("cnxregainedepsecs")) {
                    currentConnectionChangedEvents = true;
                  }
                } else if (binNumber < firstBin) {
                  // Michael Davidsaver's special case; keep track of the last
                  // value before the start time and then add that in as a
                  // single sample.
                  if (!lastSampleBeforeStartAdded) {
                    if (lastSampleBeforeStart != null) {
                      if (e.getEpochSeconds() >= lastSampleBeforeStart.getEpochSeconds()) {
                        lastSampleBeforeStart = e.makeClone();
                      }
                    } else {
                      lastSampleBeforeStart = e.makeClone();
                    }
                  }
                }
              } catch (PBParseException ex) {
                Logger.error("Skipping possible corrupted event for pv " + strm.getDescription());
              }
            }
            // The last bin with events will not yet have been added so it needs to 
            // be added manually here in order to correct for the remaining bins with
            // no events.
            if (currentBinCollector != null) {
                if (currentBinCollector.haveEventsBeenAdded()) {
                    SummaryValue summaryValue;
                    summaryValue = new SummaryValue(
                        ((SummaryStatsVectorCollector) currentBinCollector).getVectorValues(), currentMaxSeverity,
                        currentConnectionChangedEvents);
                    consolidatedData.put(currentBin, summaryValue);
                    lastBinSaved = currentBin;
                }
            }
            if (lastBinSaved != null) {
                if (lastBinSaved < lastBin) {
                    // Last bins have been skipped (had no events) so need to populate them
                    // with the last value from the last bin to have events
                    // Include the last bin (+1)
                    long nSkip = lastBin-lastBinSaved + 1;
                    populateSkippedBins(nSkip);  
                }
            }
            return new SummaryStatsCollectorEventStream(firstBin, lastBin, intervalSecs, srcDesc, consolidatedData,
                inheritValuesFromPreviousBins, zeroOutEmptyBins(), true, 6);
          }
        }
      };
    }

    /**
     * In the case that some bins have no events, fill them with the last value
     * from the last bin to be saved (i.e. that has events).
     *
     * @param nBins
     *            the number of bins to fill with last value from last bin to
     *            have values.
     */
    private void populateSkippedBins(long nBins) {
        SummaryValue oldValue = consolidatedData.get(lastBinSaved);
        double lastVal = oldValue.values.get(5);

        List<Double> list = new ArrayList<>(6);
        list.add(lastVal);
        list.add(0.0);
        list.add(lastVal);
        list.add(lastVal);
        list.add(0.0);
        list.add(lastVal);
        SummaryValue lastValSum = new SummaryValue(list, currentMaxSeverity, currentConnectionChangedEvents);

        for (int i = 1; i < nBins; i++) {
            consolidatedData.put(lastBinSaved + i, lastValSum);
        }
    }

    private void switchToNewBin(long binNumber) {
      currentBin = binNumber;
      currentMaxSeverity = 0;
      currentConnectionChangedEvents = false;
      currentBinCollector = statisticsPostProcessor.getCollector();
      currentBinCollector.setBinParams(intervalSecs, currentBin);
    }
}
