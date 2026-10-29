package org.epics.archiverappliance.retrieval.postprocessors;

import edu.stanford.slac.archiverappliance.PB.data.PBParseException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.remotable.ArrayListEventStream;
import org.epics.archiverappliance.common.remotable.RemotableEventStreamDesc;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import jakarta.servlet.http.HttpServletRequest;

/**
 * Downsamples to a requested number of points using the Largest-Triangle-Three-Buckets algorithm.
 *
 * <p>Like <code>optimized</code>, the N in <code>lttb_N</code> is a count of points, not an interval in seconds,
 * and the binning interval is derived from it. Nothing is synthesized: each bin returns one of the samples that
 * was actually archived, with its own timestamp, severity and status, the one forming the largest triangle with
 * the sample returned for the previous bin and the mean of the next bin. The first and last samples in the range
 * are always returned, and if the range holds fewer samples than were requested, all of them are. Only scalars
 * are downsampled; waveforms are returned unchanged.
 *
 * <p>The bins are spans of time rather than counts of samples, unlike the published algorithm, which bins by
 * sample index. Sizing a bin by count would need the total up front, which a stream of chunks does not have, and
 * a PV that churns for a minute and then sits idle for a day would spend nearly the whole point budget on that
 * minute. The cost is that a dense bin is reduced to one sample however much happens inside it. The bin width is
 * the range divided by the requested points, rounded down to whole seconds, so the count returned can exceed the
 * count requested.
 *
 * @author caraxlr
 */
public class LTTB implements PostProcessor, PostProcessorWithConsolidatedEventStream {

    private static Logger logger = LogManager.getLogger(LTTB.class.getName());
    private static final int DEFAULT_NUMBER_OF_POINTS = 1000;

    /** The number of points asked for, the N in lttb_N; the range is split into this many bins. */
    int numberOfPoints = DEFAULT_NUMBER_OF_POINTS;

    int intervalSecs = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;

    private String pvName;
    private Instant previousEventTimestamp = Instant.ofEpochMilli(1);

    long firstBin = 0;
    long lastBin = Long.MAX_VALUE;
    long currentBin = -1;
    RemotableEventStreamDesc srcDesc = null;

    /**
     * Only two bins are held at a time. A bin cannot be resolved until the bin after it is complete, but once it
     * is, its samples are of no further use; the one that was picked lives on as the anchor.
     */
    private LTTBBin currentLTTBBin = null;

    private LTTBBin previousLTTBBin = null;

    /** The sample returned for the previous bin; the triangles are anchored on it. */
    private Event anchor = null;

    private Event firstSample = null;
    private Event lastSample = null;
    private boolean waveform = false;
    private long numEvents = 0;

    /** The raw samples, kept only for as long as the range is smaller than the request. */
    private ArrayListEventStream rawEvents = null;

    private ArrayListEventStream selectedEvents = null;
    private EventStream consolidatedEventStream = null;

    @Override
    public String getIdentity() {
        return "lttb";
    }

    @Override
    public String getExtension() {
        String identity = this.getIdentity();
        if (numberOfPoints == DEFAULT_NUMBER_OF_POINTS) {
            return identity;
        } else {
            return identity + "_" + Integer.toString(numberOfPoints);
        }
    }

    @Override
    public void initialize(String userarg, String pvName) throws IOException {
        this.pvName = pvName;
        if (userarg != null && userarg.contains("_")) {
            String[] userparams = userarg.split("_");
            String numberStr = userparams[1];
            numberOfPoints = Integer.parseInt(numberStr);
            logger.debug("Using the user supplied point count of " + numberOfPoints);
        } else {
            logger.debug("Using the default point count of " + numberOfPoints
                    + " as the user has not specified a point count.");
        }
        if (numberOfPoints < 1) {
            throw new IOException("The point count for " + getIdentity() + " has to be at least 1 for pv " + pvName);
        }
    }

    @Override
    public long estimateMemoryConsumption(
            String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) {
        // Split the range numberOfPoints ways; each bin returns one point.
        long intervalSecsLong = (TimeUtils.convertToEpochSeconds(end) - TimeUtils.convertToEpochSeconds(start))
                / ((long) numberOfPoints);
        intervalSecs = intervalSecsLong > Integer.MAX_VALUE ? Integer.MAX_VALUE : Math.max(1, (int) intervalSecsLong);
        firstBin = TimeUtils.convertToEpochSeconds(start) / intervalSecs;
        lastBin = TimeUtils.convertToEpochSeconds(end) / intervalSecs;
        logger.debug("Expecting " + (lastBin + 2 - firstBin) + " values using an interval of " + intervalSecs);
        float storageRate = typeInfo.getComputedStorageRate();
        long numSeconds = TimeUtils.convertToEpochSeconds(end) - TimeUtils.convertToEpochSeconds(start);
        // Add a fudge factor of 2 for java
        long estimatedMemoryConsumption = (long) (storageRate * numSeconds * 2 / intervalSecs);
        return estimatedMemoryConsumption;
    }

    @Override
    public Callable<EventStream> wrap(final Callable<EventStream> callable) {
        return new Callable<EventStream>() {
            @Override
            public EventStream call() throws Exception {
                try (EventStream strm = callable.call()) {
                    RemotableEventStreamDesc strmDesc = (RemotableEventStreamDesc) strm.getDescription();
                    if (srcDesc == null) {
                        srcDesc = new RemotableEventStreamDesc(strmDesc);
                        selectedEvents = new ArrayListEventStream(numberOfPoints, srcDesc);
                        rawEvents = new ArrayListEventStream(numberOfPoints, srcDesc);
                    }
                    for (Event e : strm) {
                        try {
                            DBRTimeEvent dbrTimeEvent = (DBRTimeEvent) e;
                            if (dbrTimeEvent.getEventTimeStamp().isAfter(previousEventTimestamp)) {
                                previousEventTimestamp = dbrTimeEvent.getEventTimeStamp();
                            } else {
                                // Note that this is expected. ETL is not transactional; so we can get the same event
                                // twice from different stores.
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Skipping older event "
                                            + TimeUtils.convertToHumanReadableString(dbrTimeEvent.getEventTimeStamp())
                                            + " previous "
                                            + TimeUtils.convertToHumanReadableString(previousEventTimestamp));
                                }
                                continue;
                            }
                            addEvent(e);
                        } catch (PBParseException ex) {
                            logger.error("Skipping possible corrupted event for pv " + strmDesc);
                        }
                    }
                    // More samples have arrived, so any stream consolidated earlier is stale.
                    consolidatedEventStream = null;
                    // Nothing is decided yet, as a bin needs the next bin; the servlet discards this and reads
                    // getConsolidatedEventStream() once every source stream is in.
                    return new ArrayListEventStream(0, srcDesc);
                }
            }
        };
    }

    /** Feed one sample through the two bin window. Samples arrive in increasing time order. */
    private void addEvent(Event e) {
        numEvents++;
        lastSample = e.makeClone();
        if (numEvents == 1) {
            // The same instance, so that the bin can recognise and reserve it.
            firstSample = lastSample;
            waveform = e.getDBRType().isWaveForm();
        }
        if (numEvents <= numberOfPoints || waveform) {
            // The raw samples are handed back below the requested count, and always for a waveform.
            rawEvents.add(lastSample);
        }
        if (waveform) {
            // TODO downsample the elements within each waveform, using the element index as x.
            return;
        }

        long binNumber = e.getEpochSeconds() / intervalSecs;
        if (binNumber != currentBin) {
            switchToNewBin(binNumber);
        }
        currentLTTBBin.addEvent(lastSample);
    }

    /**
     * The bin just completed is the lookahead the bin before it was waiting on, so that one can now be resolved.
     */
    private void switchToNewBin(long binNumber) {
        if (previousLTTBBin != null) {
            selectPoint(previousLTTBBin, currentLTTBBin);
        }
        previousLTTBBin = currentLTTBBin;
        currentBin = binNumber;
        currentLTTBBin = new LTTBBin(pvName);
    }

    /**
     * The selection rule. Of the samples in the bin, the one returned is the one forming the largest triangle
     * with the anchor and the mean of the next bin; it becomes the anchor in turn. The first and the last sample
     * of the range are reserved and are never picked here.
     */
    private void selectPoint(LTTBBin bin, LTTBBin next) {
        // Mid stream lastSample belongs to a later bin than the one resolved here, so it excludes nothing.
        Event picked =
                bin.select(anchor == null ? firstSample : anchor, next == null ? bin : next, firstSample, lastSample);
        if (picked != null) {
            selectedEvents.add(picked);
            anchor = picked;
        }
    }

    /**
     * The output of the post processor; what wrap returns is discarded. The servlet calls this once every source
     * stream is in, which is the first point at which the last bin and the last sample of the range are known.
     */
    @Override
    public EventStream getConsolidatedEventStream() {
        if (consolidatedEventStream != null) {
            // Draining the window below is destructive, so the result is computed once and kept.
            return consolidatedEventStream;
        }
        if (srcDesc == null) {
            // No source stream ever arrived, so there is not even a description to hand back.
            return new ArrayListEventStream(0, this.srcDesc);
        }
        if (waveform) {
            logger.warn("The " + getIdentity() + " post processor only downsamples scalars; returning " + numEvents
                    + " raw samples for waveform pv " + pvName);
            consolidatedEventStream = new ArrayListCollectorEventStream(rawEvents);
            return consolidatedEventStream;
        }
        // True only if the raw samples were kept all the way through, so fewer arrived than were asked for.
        if (numEvents <= rawEvents.size()) {
            consolidatedEventStream = new ArrayListCollectorEventStream(rawEvents);
            return consolidatedEventStream;
        }

        // Drain the window. The final bin has nothing after it to look ahead to, so it looks at itself.
        if (previousLTTBBin != null) {
            selectPoint(previousLTTBBin, currentLTTBBin);
        }
        if (currentLTTBBin != null) {
            selectPoint(currentLTTBBin, currentLTTBBin);
        }
        // The endpoints of the range are always returned.
        selectedEvents.add(0, firstSample);
        if (lastSample != firstSample) {
            selectedEvents.add(lastSample);
        }
        consolidatedEventStream = new ArrayListCollectorEventStream(selectedEvents);
        return consolidatedEventStream;
    }

    @Override
    public long getStartBinEpochSeconds() {
        return this.firstBin * this.intervalSecs;
    }

    @Override
    public long getEndBinEpochSeconds() {
        return this.lastBin * this.intervalSecs;
    }

    @Override
    public LinkedList<TimeSpan> getBinTimestamps() {
        LinkedList<TimeSpan> ret = new LinkedList<TimeSpan>();
        long previousBinEpochSeconds = this.firstBin * this.intervalSecs;
        for (long currentBin = this.firstBin + 1; currentBin <= this.lastBin; currentBin++) {
            long currentBinEpochSeconds = currentBin * this.intervalSecs;
            ret.add(new TimeSpan(previousBinEpochSeconds, currentBinEpochSeconds));
            previousBinEpochSeconds = currentBinEpochSeconds;
        }
        return ret;
    }
}
