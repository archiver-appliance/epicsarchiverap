package org.epics.archiverappliance.retrieval.postprocessors;

import edu.stanford.slac.archiverappliance.PB.data.PBParseException;
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

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.servlet.http.HttpServletRequest;

/**
 * Abstract class for various operators that operate on a SummaryStatistics
 * Child classes implement the getIdentity and the getStats method.
 * @author mshankar
 *
 */
public abstract class SummaryStatsPostProcessor
        implements PostProcessor, PostProcessorWithConsolidatedEventStream, FillNoFillSupport {
    @Override
    public abstract String getIdentity();

    public abstract SummaryStatsCollector getCollector();

    /**
     * @return true if this post processor is providing an array (list) of data, or false if a single value is provided
     */
    public boolean isProvidingVectorData() {
        return false;
    }

    /**
     * @return the number of elements per sample that this post processor provides
     */
    public int getElementCount() {
        return 1;
    }

    private Instant start = Instant.MIN;
    private Instant end = Instant.MAX;

    private static Logger logger = LogManager.getLogger(SummaryStatsPostProcessor.class.getName());
    int intervalSecs = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;

    // Use AtomicReference for thread-safe reference updates
    private final AtomicReference<Instant> previousEventTimestamp = new AtomicReference<>(Instant.ofEpochMilli(1));

    static class SummaryValue {
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

        HashMap<String, String> additionalCols = null;

        /**
         * Summary values
         */
        List<Double> values;

        public SummaryValue(double value, int severity, boolean connectionChanged) {
            this.value = value;
            this.severity = severity;
            this.connectionChanged = connectionChanged;
        }

        public SummaryValue(List<Double> values, int severity, boolean connectionChanged) {
            this.values = values;
            this.value = Double.NaN;
            this.severity = severity;
            this.connectionChanged = connectionChanged;
        }

        public void addAdditionalColumn(HashMap<String, String> additionalColumns) {
            for (String name : additionalColumns.keySet()) {
                String value = additionalColumns.get(name);
                if (this.additionalCols == null) {
                    this.additionalCols = new HashMap<String, String>();
                }
                this.additionalCols.put(name, value);
            }
        }
    }

    // Use a lock to protect access to shared state
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Use ConcurrentHashMap for thread-safe map operations
    protected ConcurrentHashMap<Long, SummaryValue> consolidatedData = new ConcurrentHashMap<>();

    // These variables are protected by the lock
    private long firstBin = 0;
    private long lastBin = Long.MAX_VALUE;
    private long currentBin = -1;
    private int currentMaxSeverity = 0;
    private boolean currentConnectionChangedEvents = false;
    private SummaryStatsCollector currentBinCollector = null;
    private RemotableEventStreamDesc srcDesc = null;
    private boolean inheritValuesFromPreviousBins = true;
    private Event lastSampleBeforeStart = null;
    private boolean lastSampleBeforeStartAdded = false;
    private boolean shouldAddLastSampleBeforeStart = true;

    @Override
    public void initialize(String userarg, String pvName) throws IOException {
        if (userarg != null && userarg.contains("_")) {
            String[] userparams = userarg.split("_");
            String intervalStr = userparams[1];
            intervalSecs = Integer.parseInt(intervalStr);
            logger.debug("Using use supplied interval of " + intervalSecs);
        } else {
            logger.debug("Using the default interval of  " + intervalSecs
                    + " as the user has not specified the interval argument.");
        }
    }

    @Override
    public long estimateMemoryConsumption(
            String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) {
        this.start = start;
        this.end = end;

        lock.writeLock().lock();
        try {
            firstBin = TimeUtils.convertToEpochSeconds(start) / intervalSecs;
            lastBin = TimeUtils.convertToEpochSeconds(end) / intervalSecs;
        } finally {
            lock.writeLock().unlock();
        }

        logger.debug("Expecting " + lastBin + " - " + firstBin + " values "
                + (lastBin + 2 - firstBin)); // Add 2 for the first and last bins..
        float storageRate = typeInfo.getComputedStorageRate();
        long numSeconds = TimeUtils.convertToEpochSeconds(end) - TimeUtils.convertToEpochSeconds(start);
        // Add a fudge factor of 2 for java
        long estimatedMemoryConsumption = (long) (storageRate * numSeconds * 2 / intervalSecs);
        return estimatedMemoryConsumption;
    }

    @Override
    public Callable<EventStream> wrap(final Callable<EventStream> callable) {
        final boolean vectorType = isProvidingVectorData();
        final int elementCount = getElementCount();
        return new Callable<EventStream>() {
            @Override
            public EventStream call() throws Exception {
                try (EventStream strm = callable.call()) {
                    // If we cache the mean/sigma etc, then we should add something to the desc telling us that this is
                    // cached data and then we can replace the stat value for that bin?
                    lock.writeLock().lock();
                    try {
                        if (srcDesc == null) srcDesc = (RemotableEventStreamDesc) strm.getDescription();
                    } finally {
                        lock.writeLock().unlock();
                    }

                    for (Event e : strm) {
                        try {
                            DBRTimeEvent dbrTimeEvent = (DBRTimeEvent) e;

                            // Use atomic reference for thread-safe updates
                            Instant prevTimestamp = previousEventTimestamp.get();
                            if (dbrTimeEvent.getEventTimeStamp().isAfter(prevTimestamp)) {
                                previousEventTimestamp.compareAndSet(prevTimestamp, dbrTimeEvent.getEventTimeStamp());
                            } else {
                                // Note that this is expected. ETL is not transactional; so we can get the same event
                                // twice from different stores.
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Skipping older event "
                                            + TimeUtils.convertToHumanReadableString(dbrTimeEvent.getEventTimeStamp())
                                            + " previous "
                                            + TimeUtils.convertToHumanReadableString(prevTimestamp));
                                }
                                continue;
                            }

                            Instant eventInstant = dbrTimeEvent.getEventTimeStamp();
                            
                            lock.writeLock().lock();
                            try {
                                if (eventInstant.isBefore(start)) {
                                    // Michael Davidsaver's special case; keep track of the last value before the start time
                                    // and then add that in as a single sample.
                                    if (lastSampleBeforeStart == null
                                            || e.getEventTimeStamp().isAfter(lastSampleBeforeStart.getEventTimeStamp())) {
                                        lastSampleBeforeStart = e.makeClone();
                                    }
                                }
                                else if (eventInstant.isAfter(start) || eventInstant.equals(start)) {

                                    if (eventInstant.equals(start)) {
                                        shouldAddLastSampleBeforeStart = false;
                                    }

                                    // We only add bins for the specified time frame.
                                    // The ArchiveViewer depends on the number of values being the same and because of different rates
                                    // for PVs, the bin number for the starting bin could be different...
                                    // We could add a firstbin-1 and put all values before the starting timestamp in that bin but that
                                    // would give incorrect summaries.
                                    if (lastSampleBeforeStart != null && shouldAddLastSampleBeforeStart && !lastSampleBeforeStartAdded) {
                                        switchToNewBin(firstBin - 1);
                                        currentBinCollector.addEvent(lastSampleBeforeStart);
                                        lastSampleBeforeStartAdded = true;
                                    }

                                    long epochSeconds = dbrTimeEvent.getEpochSeconds();
                                    long binNumber = epochSeconds / intervalSecs;

                                    if (eventInstant.isBefore(end) || eventInstant.equals(end)) {
                                        if (binNumber != currentBin) {
                                            commitSummaryToBin(vectorType);
                                            switchToNewBin(binNumber);
                                        }
                                        currentBinCollector.addEvent(e);
                                        if (dbrTimeEvent.getSeverity() > currentMaxSeverity) {
                                            currentMaxSeverity = dbrTimeEvent.getSeverity();
                                        }
                                        if (dbrTimeEvent.hasFieldValues()
                                                && dbrTimeEvent.getFields().containsKey("cnxregainedepsecs")) {
                                            currentConnectionChangedEvents = true;
                                        }
                                    }
                                }
                            } finally {
                                lock.writeLock().unlock();
                            }
                        } catch (PBParseException ex) {
                            logger.error("Skipping possible corrupted event for pv " + strm.getDescription());
                        }
                    }

                    // If there were zero events in the timespan defined by the query,
                    // the last sample before start has not been added to a bin yet.
                    // If that is the case, add it here:
                    lock.writeLock().lock();
                    try {
                        if (lastSampleBeforeStart != null && shouldAddLastSampleBeforeStart && !lastSampleBeforeStartAdded) {
                            switchToNewBin(firstBin - 1);
                            currentBinCollector.addEvent(lastSampleBeforeStart);
                            commitSummaryToBin(vectorType);
                            lastSampleBeforeStartAdded = true;
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }

                    return new SummaryStatsCollectorEventStream(
                            lastSampleBeforeStartAdded ? firstBin - 1 : firstBin,
                            lastBin,
                            intervalSecs,
                            srcDesc,
                            consolidatedData,
                            inheritValuesFromPreviousBins,
                            zeroOutEmptyBins(),
                            vectorType,
                            elementCount);
                }
            }
        };
    }

    private void commitSummaryToBin(boolean vectorType) {
        if (currentBin != -1) {
            SummaryValue summaryValue;
            if (vectorType) {
                summaryValue = new SummaryValue(
                        ((SummaryStatsVectorCollector) currentBinCollector)
                                .getVectorValues(),
                        currentMaxSeverity,
                        currentConnectionChangedEvents);
            } else {
                summaryValue = new SummaryValue(
                        currentBinCollector.getStat(),
                        currentMaxSeverity,
                        currentConnectionChangedEvents);
                if (currentBinCollector
                        instanceof SummaryStatsCollectorAdditionalColumns) {
                    summaryValue.addAdditionalColumn(
                            ((SummaryStatsCollectorAdditionalColumns)
                                            currentBinCollector)
                                    .getAdditionalStats());
                }
            }
            consolidatedData.put(currentBin, summaryValue);
        }
    }

    private void switchToNewBin(long binNumber) {
        currentBin = binNumber;
        currentMaxSeverity = 0;
        currentConnectionChangedEvents = false;
        currentBinCollector = getCollector();
        currentBinCollector.setBinParams(intervalSecs, currentBin);
    }

    @Override
    public String getExtension() {
        String identity = this.getIdentity();
        if (intervalSecs == PostProcessors.DEFAULT_SUMMARIZING_INTERVAL) {
            return identity;
        } else {
            return identity + "_" + Integer.toString(intervalSecs);
        }
    }

    @Override
    public EventStream getConsolidatedEventStream() {
        lock.writeLock().lock();
        try {
            if (!lastSampleBeforeStartAdded && lastSampleBeforeStart != null && shouldAddLastSampleBeforeStart) {
                switchToNewBin(firstBin - 1);
                logger.debug("Adding lastSampleBeforeStart to bin "
                        + TimeUtils.convertToHumanReadableString(lastSampleBeforeStart.getEpochSeconds()));
                currentBinCollector.addEvent(lastSampleBeforeStart);
                lastSampleBeforeStartAdded = true;
            }

            if (currentBin != -1) {
                SummaryValue summaryValue;
                if (isProvidingVectorData()) {
                    summaryValue = new SummaryValue(
                            ((SummaryStatsVectorCollector) currentBinCollector).getVectorValues(),
                            currentMaxSeverity,
                            currentConnectionChangedEvents);
                } else {
                    summaryValue = new SummaryValue(
                            currentBinCollector.getStat(), currentMaxSeverity, currentConnectionChangedEvents);
                    if (currentBinCollector instanceof SummaryStatsCollectorAdditionalColumns) {
                        summaryValue.addAdditionalColumn(
                                ((SummaryStatsCollectorAdditionalColumns) currentBinCollector).getAdditionalStats());
                    }
                }
                consolidatedData.put(currentBin, summaryValue);
                currentBinCollector = null;
            }

            if (consolidatedData.isEmpty()) {
                return new ArrayListEventStream(0, srcDesc);
            } else {
                return new SummaryStatsCollectorEventStream(
                        this.firstBin == 0 ? 0 : this.firstBin - 1,
                        this.lastBin,
                        this.intervalSecs,
                        srcDesc,
                        consolidatedData,
                        inheritValuesFromPreviousBins,
                        zeroOutEmptyBins(),
                        isProvidingVectorData(),
                        getElementCount());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /* (non-Javadoc)
     * @see org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream#getStartBinEpochSeconds()
     */
    @Override
    public long getStartBinEpochSeconds() {
        lock.readLock().lock();
        try {
            return this.firstBin * this.intervalSecs;
        } finally {
            lock.readLock().unlock();
        }
    }

    /* (non-Javadoc)
     * @see org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream#getEndBinEpochSeconds()
     */
    @Override
    public long getEndBinEpochSeconds() {
        lock.readLock().lock();
        try {
            return this.lastBin * this.intervalSecs;
        } finally {
            lock.readLock().unlock();
        }
    }

    /* (non-Javadoc)
     * @see org.epics.archiverappliance.retrieval.postprocessors.PostProcessorWithConsolidatedEventStream#getBinTimestamps()
     */
    @Override
    public LinkedList<TimeSpan> getBinTimestamps() {
        lock.readLock().lock();
        try {
            return getBinTimestamps(this.firstBin, this.lastBin, this.intervalSecs);
        } finally {
            lock.readLock().unlock();
        }
    }

    public static LinkedList<TimeSpan> getBinTimestamps(long firstBin, long lastBin, int intervalSecs) {
        LinkedList<TimeSpan> ret = new LinkedList<TimeSpan>();
        long previousBinEpochSeconds = firstBin * intervalSecs;
        for (long currentBin = firstBin + 1; currentBin <= lastBin; currentBin++) {
            long currentBinEpochSeconds = currentBin * intervalSecs;
            ret.add(new TimeSpan(previousBinEpochSeconds, currentBinEpochSeconds));
            previousBinEpochSeconds = currentBinEpochSeconds;
        }
        return ret;
    }

    @Override
    public void doNotInheritValuesFromPrevioisBins() {
        lock.writeLock().lock();
        try {
            this.inheritValuesFromPreviousBins = false;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean zeroOutEmptyBins() {
        return false;
    }}
