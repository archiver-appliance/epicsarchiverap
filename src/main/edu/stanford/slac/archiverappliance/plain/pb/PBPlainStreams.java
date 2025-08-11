package edu.stanford.slac.archiverappliance.plain.pb;

import edu.stanford.slac.archiverappliance.plain.PlainStreams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BiDirectionalIterable;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.FieldValues;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.retrieval.channelarchiver.HashMapEvent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class PBPlainStreams implements PlainStreams {
    private static final Logger logger = LogManager.getLogger(PBPlainStreams.class);
    /* A file that has at most a few events and is faster when loaded completely in memory */
    public static int SIZE_THAT_DETERMINES_A_SMALL_FILE = 4 * 1024;

    public PBFileInfo fileInfo(Path path) throws IOException {
        return new PBFileInfo(path);
    }

    @Override
    public EventStream getTimeStream(
            String pvName, Path path, ArchDBRTypes dbrType, Instant start, Instant end, boolean skipSearch)
            throws IOException {
        return new FileBackedPBEventStream(pvName, path, dbrType, start, end, skipSearch);
    }

    @Override
    public EventStream getTimeStream(
            String pvName, Path path, Instant start, Instant end, boolean skipSearch, PBFileInfo fileInfo)
            throws IOException {
        return new FileBackedPBEventStream(pvName, path, fileInfo.getType(), start, end, skipSearch);
    }

    @Override
    public EventStream getStream(String pvName, Path path, ArchDBRTypes dbrType) throws IOException {
        return new FileBackedPBEventStream(pvName, path, dbrType);
    }

    public static EventStream getStreamForIteration(
            String pvName,
            Path path,
            Instant startAtTime,
            ArchDBRTypes archDBRTypes,
            BiDirectionalIterable.IterationDirection direction)
            throws IOException {
        if (Files.size(path) < SIZE_THAT_DETERMINES_A_SMALL_FILE) {
            return new ArrayListEventStreamWithPositionedIterator(pvName, path, startAtTime, archDBRTypes, direction);
        }

        return new FileBackedPBEventStream(pvName, path, archDBRTypes, startAtTime, direction);
    }

    public Event findByTime(
            List<Path> pathList,
            String pvName,
            Instant atTime,
            Instant startAtTime,
            BiDirectionalIterable.IterationDirection direction)
            throws IOException {

        for (Path path : pathList) {
            logger.info("Iterating thru {}", path);
            PBFileInfo fileInfo = fileInfo(path);
            try (EventStream strm = getStreamForIteration(pvName, path, startAtTime, fileInfo.getType(), direction)) {
                Event e = findByTimeInStream(strm, atTime, direction);
                if (e != null) {
                    return e;
                }
            }
        }
        return null;
    }

    private static boolean foundEvent(
            Instant eventTime, Instant atTime, BiDirectionalIterable.IterationDirection direction) {
        return (direction == BiDirectionalIterable.IterationDirection.BACKWARDS && eventTime.isBefore(atTime))
                || (direction == BiDirectionalIterable.IterationDirection.FORWARDS && eventTime.isAfter(atTime))
                || eventTime.equals(atTime);
    }

    private static Event findByTimeInStream(
            EventStream strm, Instant atTime, BiDirectionalIterable.IterationDirection direction) throws IOException {
        Instant maxMetaDataTimeBefore = atTime.minus(ArchiveChannel.SAVE_META_DATA_PERIOD_SECS, ChronoUnit.SECONDS);
        HashMapEvent resultEvent = null;
        for (Event event : strm) {
            if (resultEvent == null && foundEvent(event.getEventTimeStamp(), atTime, direction)) {
                if (event instanceof DBRTimeEvent dbrTimeEvent) {
                    resultEvent = new HashMapEvent(strm.getDescription().getArchDBRType(), dbrTimeEvent);
                }
            }
            if (resultEvent != null && event instanceof FieldValues fv) {
                copyNotSetFieldValues(fv, resultEvent);
            }
            if (event.getEventTimeStamp().isBefore(maxMetaDataTimeBefore)) {
                break;
            }
        }
        return resultEvent;
    }

    private static void copyNotSetFieldValues(FieldValues fv, HashMapEvent resultEvent) {
        var evFields = fv.getFields();
        if (evFields != null && !evFields.isEmpty()) {
            for (Map.Entry<String, String> fieldValue : evFields.entrySet()) {
                String fieldValueValue = resultEvent.getFieldValue(fieldValue.getKey());
                if (fieldValueValue == null) {
                    resultEvent.addFieldValue(fieldValue.getKey(), fieldValue.getValue());
                }
            }
        }
    }

    @Override
    public Event dataAtTime(
            List<Path> pathList,
            String pvName,
            Instant atTime,
            Instant startAtTime,
            BiDirectionalIterable.IterationDirection direction)
            throws IOException {
        return findByTime(pathList, pvName, atTime, startAtTime, direction);
    }
}
