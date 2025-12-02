package edu.stanford.slac.archiverappliance.plain.pb;

import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import edu.stanford.slac.archiverappliance.plain.PathResolver;
import edu.stanford.slac.archiverappliance.plain.PlainFileHandler;
import edu.stanford.slac.archiverappliance.plain.URLKey;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.BiDirectionalIterable;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.FieldValues;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.common.DefaultETLInfoListProcessor;
import org.epics.archiverappliance.etl.common.ETLInfoListProcessor;
import org.epics.archiverappliance.retrieval.channelarchiver.HashMapEvent;
import org.epics.archiverappliance.utils.nio.ArchPaths;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class PBPlainFileHandler implements PlainFileHandler {
    public static final String PB_PLUGIN_IDENTIFIER = "pb";
    public static final String pbFileExtension = ".pb";
    public static int SIZE_THAT_DETERMINES_A_SMALL_FILE = 4 * 1024;

    private PBCompressionMode compressionMode = PBCompressionMode.NONE;

    @Override
    public String pluginIdentifier() {
        return PB_PLUGIN_IDENTIFIER;
    }

    @Override
    public PathResolver getPathResolver() {
        return switch (compressionMode) {
            case NONE -> PathResolver.BASE_PATH_RESOLVER;
            case ZIP_PER_PV -> (paths, createParentFolder, rootFolder, pvComponent, pvKey) ->
                    paths.get(createParentFolder, rootFolder, pvKey + "_pb.zip!", pvComponent);
        };
    }

    @Override
    public boolean useSearchForPositions() {
        return this.compressionMode.equals(PBCompressionMode.NONE);
    }

    @Override
    public String rootFolderPath(String rootFolder) {
        return this.compressionMode.equals(PBCompressionMode.NONE)
                ? rootFolder
                : rootFolder.replace(ArchPaths.ZIP_PREFIX, "/");
    }

    @Override
    public void initCompression(Map<String, String> queryStrings) {
        this.compressionMode = PBCompressionMode.valueOf(queryStrings.get(URLKey.COMPRESS.key()));
    }

    @Override
    public FileInfo fileInfo(Path path) throws IOException {
        return new PBFileInfo(path);
    }

    @Override
    public String toString() {
        return "PBPlainFileHandler{" + "compressionMode=" + compressionMode + '}';
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
            PVNameToKeyMapping pv2key) {
        return new PBAppendDataStateData(
                partitionGranularity, rootFolder, desc, timestamp, compressionMode, pv2key, getPathResolver());
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
            PVNameToKeyMapping pv2key)
            throws IOException {
        PlainFileHandler.movePaths(context, pvName, randSuffix, suffix, rootFolder, getPathResolver(), pv2key);
    }

    @Override
    public void dataDeleteTempFiles(
            BasicContext context, String pvName, String randSuffix, String rootFolder, PVNameToKeyMapping pv2key)
            throws IOException {
        PlainFileHandler.deleteTempFiles(context, pvName, randSuffix, rootFolder, getPathResolver(), pv2key);
    }

    @Override
    public ETLInfoListProcessor optimisedETLInfoListProcessor(ETLDest etlDest) {
        return new DefaultETLInfoListProcessor(etlDest);
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
            FileInfo fileInfo = fileInfo(path);
            try (EventStream strm = getStreamForIteration(pvName, path, startAtTime, fileInfo.getType(), direction)) {
                Event e = findByTimeInStream(strm, atTime, direction);
                if (e != null) {
                    return e;
                }
            }
        }
        return null;
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

    private static boolean foundEvent(
            Instant eventTime, Instant atTime, BiDirectionalIterable.IterationDirection direction) {
        return (direction == BiDirectionalIterable.IterationDirection.BACKWARDS && eventTime.isBefore(atTime))
                || (direction == BiDirectionalIterable.IterationDirection.FORWARDS && eventTime.isAfter(atTime))
                || eventTime.equals(atTime);
    }

    private static Event findByTimeInStream(
            EventStream strm, Instant atTime, BiDirectionalIterable.IterationDirection direction) throws IOException {
        HashMapEvent resultEvent = null;
        boolean foundFieldValues = false;
        boolean passedFoundEvent = false;
        boolean foundAnyEvent = false;
        for (Event event : strm) {
            boolean foundEvent = foundEvent(event.getEventTimeStamp(), atTime, direction);
            if (resultEvent == null && foundEvent && event instanceof DBRTimeEvent dbrTimeEvent) {
                resultEvent = new HashMapEvent(strm.getDescription().getArchDBRType(), dbrTimeEvent);
            }
            if (resultEvent != null && event instanceof FieldValues fv) {
                copyNotSetFieldValues(fv, resultEvent);
                foundFieldValues = true;
            }
            if (foundEvent) {
                foundAnyEvent = true;
            }
            if (!foundEvent && foundAnyEvent) {
                passedFoundEvent = true;
            }
            if (passedFoundEvent && foundFieldValues) {
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

    @Override
    public String updateRootFolderStr(String rootFolderStr) {
        if (compressionMode.equals(PBCompressionMode.ZIP_PER_PV)) {
            if (!rootFolderStr.startsWith(ArchPaths.ZIP_PREFIX)) {
                String rootFolderWithPath = ArchPaths.ZIP_PREFIX + rootFolderStr;
                logger.debug("Automatically adding url scheme for compression to rootfolder " + rootFolderWithPath);
                return rootFolderWithPath;
            }
        }
        return rootFolderStr;
    }

    @Override
    public boolean backUpFiles(boolean backupFilesBeforeETL) {
        return this.compressionMode.equals(PBCompressionMode.NONE) && backupFilesBeforeETL;
    }

    @Override
    public Map<URLKey, String> urlOptions() {
        if (compressionMode.equals(PBCompressionMode.NONE)) {
            return Map.of();
        }
        return Map.of(URLKey.COMPRESS, compressionMode.name());
    }

    @Override
    public String getPathKey(Path path) {
        if (this.compressionMode.equals(PBCompressionMode.NONE)) {
            return path.toAbsolutePath().toString();
        }
        return URLDecoder.decode(path.toUri().toString(), StandardCharsets.US_ASCII)
                .replace(" ", "+");
    }
}
