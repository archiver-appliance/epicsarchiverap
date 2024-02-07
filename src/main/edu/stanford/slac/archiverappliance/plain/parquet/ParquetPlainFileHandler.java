package edu.stanford.slac.archiverappliance.plain.parquet;

import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.CompressionMode;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import edu.stanford.slac.archiverappliance.plain.PlainFileHandler;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVNameToKeyMapping;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public class ParquetPlainFileHandler implements PlainFileHandler {

    public static final String PARQUET_PLUGIN_IDENTIFIER = "parquet";

    @Override
    public String pluginIdentifier() {
        return PARQUET_PLUGIN_IDENTIFIER;
    }

    @Override
    public String toString() {
        return "ParquetPlainFileHandler{}";
    }

    @Override
    public FileInfo fileInfo(Path path) throws IOException {
        return new ParquetInfo(path);
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
    public AppendDataStateData appendDataStateData(
            Instant timestamp,
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            PVNameToKeyMapping pv2key,
            CompressionMode compressionMode) {
        return new ParquetAppendDataStateData(
                partitionGranularity, rootFolder, desc, timestamp, compressionMode, pv2key);
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
            CompressionMode compressionMode,
            PVNameToKeyMapping pv2key)
            throws IOException {
        PlainFileHandler.movePaths(context, pvName, randSuffix, suffix, rootFolder, compressionMode, pv2key);
        PlainFileHandler.movePaths(
                context,
                "." + pvName,
                randSuffix,
                getExtensionString() + randSuffix + ".crc",
                rootFolder,
                compressionMode,
                pv2key);
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
        PlainFileHandler.deleteTempFiles(
                context, "." + pvName, randSuffix + ".crc", rootFolder, compressionMode, pv2key);
    }
}
