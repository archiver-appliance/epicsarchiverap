package edu.stanford.slac.archiverappliance.plain.pb;

import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.CompressionMode;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import edu.stanford.slac.archiverappliance.plain.PlainFileHandler;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.BiDirectionalIterable;
import org.epics.archiverappliance.common.BiDirectionalIterable.IterationDirection;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.FieldValues;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.common.DefaultETLInfoListProcessor;
import org.epics.archiverappliance.etl.common.ETLInfoListProcessor;
import org.epics.archiverappliance.retrieval.channelarchiver.HashMapEvent;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class PBPlainFileHandler implements PlainFileHandler {
    public static final String PB_PLUGIN_IDENTIFIER = "pb";
    public static final String pbFileExtension = ".pb";

    @Override
    public String pluginIdentifier() {
        return PB_PLUGIN_IDENTIFIER;
    }

    @Override
    public FileInfo fileInfo(Path path) throws IOException {
        return new PBFileInfo(path);
    }

    @Override
    public String toString() {
        return "PBPlainFileHandler{}";
    }

    @Override
    public EventStream getTimeStream(
            String pvName, Path path, ArchDBRTypes dbrType, Instant start, Instant end, boolean skipSearch)
            throws IOException {
        return new FileBackedPBEventStream(pvName, path, dbrType, start, end, skipSearch);
    }

    @Override
    public EventStream getTimeStream(
            String pvName, Path path, Instant start, Instant end, boolean skipSearch, FileInfo fileInfo)
            throws IOException {
        return new FileBackedPBEventStream(pvName, path, fileInfo.getType(), start, end, skipSearch);
    }

    @Override
    public EventStream getStream(String pvName, Path path, ArchDBRTypes dbrType) throws IOException {
        return new FileBackedPBEventStream(pvName, path, dbrType);
    }

    @Override
    public AppendDataStateData appendDataStateData(
            Instant timestamp,
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            PVNameToKeyMapping pv2key,
            CompressionMode compressionMode) {
        return new PBAppendDataStateData(partitionGranularity, rootFolder, desc, timestamp, compressionMode, pv2key);
    }

    @Override
    public void markForDeletion(Path path) {
        // Nothing for PB files
    }

    @Override
    public void dataMovePaths(
            BasicContext context,
            String pvName,
            String randSuffix,
            String suffix,
            String rootFolder,
            CompressionMode compressionMode,
            PVNameToKeyMapping pv2key)
            throws IOException {
        PlainFileHandler.movePaths(context, pvName, randSuffix, suffix, rootFolder, compressionMode, pv2key);
    }

    @Override
    public void dataDeleteTempFiles(
            BasicContext context,
            String pvName,
            String randSuffix,
            String rootFolder,
            CompressionMode compressionMode,
            PVNameToKeyMapping pv2key)
            throws IOException {
        PlainFileHandler.deleteTempFiles(context, pvName, randSuffix, rootFolder, compressionMode, pv2key);
    }

    @Override
    public ETLInfoListProcessor optimisedETLInfoListProcessor(ETLDest etlDest) {
        return new DefaultETLInfoListProcessor(etlDest);
    }

    public Event findByTime(
            List<Path> pathList, String pvName, Instant atTime, Instant startAtTime, IterationDirection direction)
            throws IOException {

        for (Path path : pathList) {
            logger.info("Iterating thru {}", path);
            FileInfo fileInfo = fileInfo(path);
            try (EventStream strm =
                    new FileBackedPBEventStream(pvName, path, fileInfo.getType(), startAtTime, direction)) {
                Event e = findByTimeInStream(strm, atTime, direction);
                if (e != null) {
                    return e;
                }
            }
        }
        return null;
    }

    private static boolean foundEvent(Instant eventTime, Instant atTime, IterationDirection direction) {
        return (direction == IterationDirection.BACKWARDS && eventTime.isBefore(atTime))
                || (direction == IterationDirection.FORWARDS && eventTime.isAfter(atTime))
                || eventTime.equals(atTime);
    }

    private static Event findByTimeInStream(EventStream strm, Instant atTime, IterationDirection direction)
            throws IOException {
        Instant maxMetaDataTimeBefore = atTime.minus(ArchiveChannel.SAVE_META_DATA_PERIOD_SECS, ChronoUnit.SECONDS);
        HashMapEvent event = null;
        for (Event ev : strm) {
            if (foundEvent(ev.getEventTimeStamp(), atTime, direction)) {
                if (ev instanceof DBRTimeEvent dbrTimeEvent) {
                    event = new HashMapEvent(strm.getDescription().getArchDBRType(), dbrTimeEvent);
                }
            }
            if (event != null && ev instanceof FieldValues fv) {
                var evFields = fv.getFields();
                if (evFields != null && !evFields.isEmpty()) {
                    for (Map.Entry<String, String> fieldValue : evFields.entrySet()) {
                        event.addFieldValue(fieldValue.getKey(), fieldValue.getValue());
                    }
                }
            }
            if (ev.getEventTimeStamp().isBefore(maxMetaDataTimeBefore)) {
                break;
            }
        }
        return event;
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
