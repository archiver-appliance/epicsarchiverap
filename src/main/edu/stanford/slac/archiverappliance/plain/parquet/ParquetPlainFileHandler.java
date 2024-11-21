package edu.stanford.slac.archiverappliance.plain.parquet;

import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import edu.stanford.slac.archiverappliance.plain.PathResolver;
import edu.stanford.slac.archiverappliance.plain.PlainFileHandler;
import edu.stanford.slac.archiverappliance.plain.URLKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.BiDirectionalIterable;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.FieldValues;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.common.ETLInfoListProcessor;
import org.epics.archiverappliance.retrieval.channelarchiver.HashMapEvent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

public class ParquetPlainFileHandler implements PlainFileHandler {

    public static final String PARQUET_PLUGIN_IDENTIFIER = "parquet";
    public static final String ZSTD_BUFFER_POOL_ENABLED = "parquet.compression.codec.zstd.bufferPool.enabled";
    public static final String ZSTD_LEVEL = "parquet.compression.codec.zstd.level";
    public static final String ZSTD_WORKERS = "parquet.compression.codec.zstd.workers";
    private final Configuration hadoopConfiguration = new Configuration();

    private CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;

    public ParquetPlainFileHandler() {
        hadoopConfiguration.set("parquet.writer.version", "2");
    }

    @Override
    public String toString() {
        return "ParquetPlainFileHandler{" + "hadoopConfiguration="
                + hadoopConfiguration + ", compressionCodecName="
                + compressionCodecName + '}';
    }

    @Override
    public String pluginIdentifier() {
        return PARQUET_PLUGIN_IDENTIFIER;
    }

    @Override
    public PathResolver getPathResolver() {
        return PathResolver.BASE_PATH_RESOLVER;
    }

    @Override
    public boolean useSearchForPositions() {
        return true;
    }

    @Override
    public String rootFolderPath(String rootFolder) {
        return rootFolder;
    }

    @Override
    public void initCompression(Map<String, String> queryStrings) {

        compressionCodecName = CompressionCodecName.valueOf(queryStrings.get(URLKeys.COMPRESS.key()));
        if (compressionCodecName.equals(CompressionCodecName.ZSTD)) {

            if (queryStrings.containsKey(URLKeys.ZSTD_BUFFER_POOL.key())) {
                hadoopConfiguration.set(ZSTD_BUFFER_POOL_ENABLED, queryStrings.get(URLKeys.ZSTD_BUFFER_POOL.key()));
            }
            if (queryStrings.containsKey(URLKeys.ZSTD_LEVEL.key())) {
                hadoopConfiguration.set(ZSTD_LEVEL, queryStrings.get(URLKeys.ZSTD_LEVEL.key()));
            }
            if (queryStrings.containsKey(URLKeys.ZSTD_WORKERS.key())) {
                hadoopConfiguration.set(ZSTD_WORKERS, queryStrings.get(URLKeys.ZSTD_WORKERS.key()));
            }
        }
    }

    @Override
    public FileInfo fileInfo(Path path) throws IOException {
        return new ParquetInfo(path, hadoopConfiguration);
    }

    @Override
    public EventStream getTimeStream(
            String pvName, Path path, ArchDBRTypes dbrType, Instant start, Instant end, boolean skipSearch)
            throws IOException {
        return new ParquetBackedPBEventFileStream(pvName, List.of(path), dbrType, start, end);
    }

    @Override
    public EventStream getTimeStream(
            String pvName, Path path, Instant start, Instant end, boolean skipSearch, FileInfo fileInfo)
            throws IOException {
        return new ParquetBackedPBEventFileStream(
                pvName, List.of(path), fileInfo.getType(), start, end, (ParquetInfo) fileInfo);
    }

    @Override
    public EventStream getStream(String pvName, Path path, ArchDBRTypes dbrType) throws IOException {
        return new ParquetBackedPBEventFileStream(pvName, path, dbrType);
    }

    @Override
    public Event dataAtTime(
            List<Path> pathList,
            String pvName,
            Instant atTime,
            Instant startAtTime,
            BiDirectionalIterable.IterationDirection iterationDirection)
            throws IOException {
        for (Path path : pathList) {
            ParquetInfo parquetInfo = new ParquetInfo(path);
            YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(atTime);
            DBRTimeEvent timeEvent = parquetInfo.getLastEventBefore(yst);
            if (timeEvent != null) {
                return fillInLatestMetadata(pvName, timeEvent, pathList, parquetInfo);
            }
        }
        return null;
    }

    private Event fillInLatestMetadata(
            String pvName, DBRTimeEvent timeEvent, List<Path> pathList, ParquetInfo parquetInfo) throws IOException {
        Instant maxMetaDataTimeAfter =
                timeEvent.getEventTimeStamp().minus(ArchiveChannel.SAVE_META_DATA_PERIOD_SECS, ChronoUnit.SECONDS);
        DBRTimeEvent hashMapEvent = new HashMapEvent(timeEvent.getDBRType(), timeEvent);
        try (EventStream eventStream = new ParquetBackedPBEventFileStream(
                pvName,
                pathList,
                timeEvent.getDBRType(),
                maxMetaDataTimeAfter,
                timeEvent.getEventTimeStamp(),
                parquetInfo)) {
            for (Event event : eventStream) {
                if (event instanceof FieldValues fv) {
                    var evFields = fv.getFields();
                    if (evFields != null && !evFields.isEmpty()) {
                        for (Map.Entry<String, String> fieldValue : evFields.entrySet()) {
                            hashMapEvent.addFieldValue(fieldValue.getKey(), fieldValue.getValue());
                        }
                    }
                }
            }
        }
        return hashMapEvent;
    }

    @Override
    public AppendDataStateData appendDataStateData(
            Instant timestamp,
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            PVNameToKeyMapping pv2key) {
        return new ParquetAppendDataStateData(
                partitionGranularity,
                rootFolder,
                desc,
                timestamp,
                this.compressionCodecName,
                pv2key,
                hadoopConfiguration,
                this.getPathResolver());
    }

    @Override
    public void markForDeletion(Path path) throws IOException {
        Path checkSumPath = Path.of(String.valueOf(path.getParent()), "." + path.getFileName() + ".crc");
        if (Files.exists(checkSumPath)) {
            Files.delete(checkSumPath);
        }
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
        PlainFileHandler.movePaths(
                context,
                "." + pvName,
                randSuffix,
                getExtensionString() + randSuffix + ".crc",
                rootFolder,
                getPathResolver(),
                pv2key);
    }

    @Override
    public void dataDeleteTempFiles(
            BasicContext context, String pvName, String randSuffix, String rootFolder, PVNameToKeyMapping pv2key)
            throws IOException {
        PlainFileHandler.deleteTempFiles(context, pvName, randSuffix, rootFolder, getPathResolver(), pv2key);
        PlainFileHandler.deleteTempFiles(
                context, "." + pvName, randSuffix + ".crc", rootFolder, getPathResolver(), pv2key);
    }

    @Override
    public ETLInfoListProcessor optimisedETLInfoListProcessor(ETLDest etlDest) {
        return new ParquetETLInfoListProcessor(etlDest);
    }

    @Override
    public String updateRootFolderStr(String rootFolderStr) {
        return rootFolderStr;
    }

    @Override
    public boolean backUpFiles(boolean backupFilesBeforeETL) {
        return backupFilesBeforeETL;
    }

    private static void updateMap(
            URLKeys urlKey,
            String configKey,
            Configuration configuration,
            Map<URLKeys, String> map,
            String defaultValue) {
        String value = configuration.get(configKey);
        if (value != null && !value.equals(defaultValue)) {
            map.put(urlKey, value);
        }
    }

    @Override
    public Map<URLKeys, String> urlOptions() {

        if (compressionCodecName.equals(CompressionCodecName.ZSTD)) {
            Map<URLKeys, String> map = new HashMap<>();
            map.put(URLKeys.COMPRESS, compressionCodecName.name());
            updateMap(URLKeys.ZSTD_BUFFER_POOL, ZSTD_BUFFER_POOL_ENABLED, hadoopConfiguration, map, "false");
            updateMap(URLKeys.ZSTD_LEVEL, ZSTD_LEVEL, hadoopConfiguration, map, "3");
            updateMap(URLKeys.ZSTD_WORKERS, ZSTD_WORKERS, hadoopConfiguration, map, "0");
        } else if (!compressionCodecName.equals(CompressionCodecName.UNCOMPRESSED)) {
            return Map.of(URLKeys.COMPRESS, compressionCodecName.name());
        }
        return Map.of();
    }

    @Override
    public String getPathKey(Path path) {
        return path.toAbsolutePath().toString();
    }
}
