package org.epics.archiverappliance.retrieval.postprocessors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.POJOEvent;
import org.epics.archiverappliance.common.TimeSpan;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.AlarmInfo;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import jakarta.servlet.http.HttpServletRequest;

/**
 *
 * <code>Nth</code> is a post processor which returns every n-th value.
 *
 * @author <a href="mailto:jaka.bobnar@cosylab.com">Jaka Bobnar</a>
 *
 */
public class Nth implements PostProcessor, PostProcessorWithConsolidatedEventStream {

    private static final String IDENTITY = "nth";
    public static final int MAX_COUNT = Integer.MAX_VALUE / 1000;

    private static Logger logger = LogManager.getLogger(Nth.class.getName());

    private int everyNth = PostProcessors.DEFAULT_SUMMARIZING_INTERVAL;
    private int count;
    private long startTime;
    private long endTime;
    private ArrayListEventStream data;
    private Instant previousEventTimestamp = Instant.ofEpochMilli(1);
    private int i = 0;

    @Override
    public String getIdentity() {
        return IDENTITY;
    }

    @Override
    public String getExtension() {
        String identity = this.getIdentity();
        if (everyNth == PostProcessors.DEFAULT_SUMMARIZING_INTERVAL) {
            return identity;
        } else {
            return identity + "_" + Integer.toString(everyNth);
        }
    }

    @Override
    public void initialize(String userarg, String pvName) throws IOException {
        if (userarg != null && userarg.contains("_")) {
            String[] userparams = userarg.split("_");
            String everyNthStr = userparams[1];
            everyNth = Integer.parseInt(everyNthStr);
            logger.debug("Using the supplied n " + everyNthStr);
        } else {
            logger.debug("Using the default n " + everyNth + " as the user has not specified the n argument.");
        }
    }

    @Override
    public long estimateMemoryConsumption(
            String pvName, PVTypeInfo typeInfo, Instant start, Instant end, HttpServletRequest req) {
        this.startTime = start.toEpochMilli();
        this.endTime = end.toEpochMilli();
        long startTime = TimeUtils.convertToEpochSeconds(start);
        long endTime = TimeUtils.convertToEpochSeconds(end);
        count = (int) ((endTime - startTime) / typeInfo.getSamplingPeriod()) / everyNth;
        // limit the count to ~2M (2.147.483) to avoid out of memory errors
        if (count > MAX_COUNT)
            logger.warn("Too many points expected (" + count + "). The returned data array will be cut off at "
                    + MAX_COUNT + " points.");

        if (count < 0) count = 1;
        else count = Math.min(MAX_COUNT, count);
        logger.debug(count + " number of points expected.");
        return (long) (typeInfo.getComputedStorageRate() * (endTime - startTime) * 2 / everyNth);
    }

    @Override
    public Callable<EventStream> wrap(final Callable<EventStream> callable) {
        // This method might be called several times with different parameter, each containing a subset of points
        // To handle the points correctly and to ensure that the points are ordered by the timestamps, hold
        // the running integer (i) and the previous timestamp (previousEventTimestamp) globally.
        return new Callable<EventStream>() {
            @Override
            public EventStream call() throws Exception {
                try (EventStream strm = callable.call()) {
                    if (data == null) {
                        data = new ArrayListEventStream(count, (RemotableEventStreamDesc) strm.getDescription());
                    }
                    for (Event e : strm) {
                        if (e.getEventTimeStamp().isAfter(previousEventTimestamp)) {
                            previousEventTimestamp = e.getEventTimeStamp();
                        } else {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Skipping older event "
                                        + TimeUtils.convertToHumanReadableString(e.getEventTimeStamp()) + " previous "
                                        + TimeUtils.convertToHumanReadableString(previousEventTimestamp));
                            }
                            continue;
                        }
                        long s = e.getEventTimeStamp().toEpochMilli();
                        if (s < startTime || s > endTime) {
                            logger.debug("Skipping event that is out of selected boundaries. Time: "
                                    + TimeUtils.convertToHumanReadableString(s));
                        } else {
                            if (i++ % everyNth == 0) {
                                // Transform events to POJOEvent. Using the incomming events causes troubles when
                                // transferring data to the client (client only sees one sample).
                                if (e instanceof AlarmInfo) {
                                    data.add(new POJOEvent(
                                            e.getDBRType(),
                                            e.getEventTimeStamp(),
                                            e.getSampleValue(),
                                            ((AlarmInfo) e).getStatus(),
                                            ((AlarmInfo) e).getSeverity()));
                                } else {
                                    data.add(new POJOEvent(
                                            e.getDBRType(), e.getEventTimeStamp(), e.getSampleValue(), 0, 0));
                                }
                            }
                        }
                        if (data.size() == MAX_COUNT) {
                            logger.warn("Too many points. Truncating the data array at " + MAX_COUNT);
                            break;
                        }
                    }
                    return new ArrayListEventStream(0, (RemotableEventStreamDesc) strm.getDescription());
                }
            }
        };
    }

    @Override
    public LinkedList<TimeSpan> getBinTimestamps() {
        LinkedList<TimeSpan> list = new LinkedList<>();
        if (data == null) return list;
        for (int i = 0; i < data.size() - 1; i++) {
            list.add(new TimeSpan(
                    data.get(i).getEventTimeStamp(), data.get(i + 1).getEventTimeStamp()));
        }
        return list;
    }

    @Override
    public EventStream getConsolidatedEventStream() {
        return data;
    }

    @Override
    public long getEndBinEpochSeconds() {
        return TimeUtils.convertToEpochSeconds(Instant.ofEpochMilli(endTime));
    }

    @Override
    public long getStartBinEpochSeconds() {
        return TimeUtils.convertToEpochSeconds(Instant.ofEpochMilli(startTime));
    }
}
