package edu.stanford.slac.archiverappliance.plain;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.etl.ETLDest;
import org.epics.archiverappliance.etl.common.ETLInfoListProcessor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.Map;

public interface PlainFileHandler extends PlainStreams {
    Logger logger = LogManager.getLogger(PlainFileHandler.class.getName());

    String pluginIdentifier();

    PathResolver getPathResolver();

    default String getExtensionString() {
        return "." + pluginIdentifier();
    }

    boolean useSearchForPositions();

    String rootFolderPath(String rootFolder);

    void initCompression(Map<String, String> queryStrings);

    FileInfo fileInfo(Path path) throws IOException;

    AppendDataStateData appendDataStateData(
            Instant timestamp,
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            PVNameToKeyMapping pv2key);

    EventFileWriter createEventFileWriter(String pvName, Path path, ArchDBRTypes type, short year) throws IOException;

    void markForDeletion(Path path) throws IOException;

    static void movePaths(
            BasicContext context,
            String pvName,
            String randSuffix,
            String suffix,
            String rootFolder,
            PathResolver pathResolver,
            PVNameToKeyMapping pv2key)
            throws IOException {
        Path[] paths =
                PathNameUtility.getAllPathsForPV(context.getPaths(), rootFolder, pvName, suffix, pathResolver, pv2key);
        for (Path path : paths) {
            Path destPath = context.getPaths().get(path.toString().replace(randSuffix, ""));
            logger.debug("Moving path " + path + " to " + destPath);
            Files.move(path, destPath, StandardCopyOption.ATOMIC_MOVE);
        }
    }

    static void deleteTempFiles(
            BasicContext context,
            String pvName,
            String randSuffix,
            String rootFolder,
            PathResolver pathResolver,
            PVNameToKeyMapping pv2key)
            throws IOException {
        Path[] paths = PathNameUtility.getAllPathsForPV(
                context.getPaths(), rootFolder, pvName, randSuffix, pathResolver, pv2key);
        for (Path path : paths) {
            logger.error("Deleting leftover file " + path);
            Files.delete(path);
        }
    }

    void dataMovePaths(
            BasicContext context,
            String pvName,
            String randSuffix,
            String suffix,
            String rootFolder,
            PVNameToKeyMapping pv2key)
            throws IOException;

    void dataDeleteTempFiles(
            BasicContext context, String pvName, String randSuffix, String rootFolder, PVNameToKeyMapping pv2key)
            throws IOException;

    ETLInfoListProcessor optimisedETLInfoListProcessor(ETLDest etlDest);

    String updateRootFolderStr(String rootFolderStr);

    boolean backUpFiles(boolean backupFilesBeforeETL);

    Map<URLKey, String> urlOptions();

    String getPathKey(Path path);
}
