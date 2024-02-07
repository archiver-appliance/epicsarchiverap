package edu.stanford.slac.archiverappliance.plain.pb;

import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.CompressionMode;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import edu.stanford.slac.archiverappliance.plain.PlainFileHandler;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.BiDirectionalIterable;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.common.DefaultETLInfoListProcessor;
import org.epics.archiverappliance.etl.common.ETLInfoListProcessor;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;

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
    public EventStream getStreamForIteration(
            String pvName,
            Path path,
            Instant startAtTime,
            ArchDBRTypes type,
            BiDirectionalIterable.IterationDirection direction)
            throws IOException {
        return new FileBackedPBEventStream(pvName, path, type, startAtTime, direction);
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
}
