package edu.stanford.slac.archiverappliance.plain.parquet;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBMessageTypeMapping;
import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.PathResolver;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.rewrite.ParquetRewriter;
import org.apache.parquet.hadoop.rewrite.RewriteOptions;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.etl.ETLBulkStream;
import org.epics.archiverappliance.etl.ETLContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * ParquetAppendDataStateData extends AppendDataStateData to support writing data to Parquet files.
 */
public class ParquetAppendDataStateData extends AppendDataStateData {
    private static final Logger logger = LogManager.getLogger(ParquetAppendDataStateData.class.getName());
    private static final String TEMP_FILE_PREFIX = "~";
    private final Configuration configuration;
    private final CompressionCodecName compressionCodecName;
    EpicsParquetWriter.Builder<Object> writerBuilder;
    private Path tempFile;
    private ParquetWriter<Object> writer;

    /**
     * ParquetAppendDataStateData extends AppendDataStateData to support writing data to Parquet files.
     *
     * @param partitionGranularity partitionGranularity of the PB plugin.
     * @param rootFolder           RootFolder of the PB plugin
     * @param desc                 Desc for logging purposes
     * @param lastKnownTimestamp   This is probably the most important argument here.
     *                             This is the last known timestamp in this storage.
     *                             If null, we assume time(0) for the last known timestamp.
     * @param pv2key               PVNameToKeyMapping
     */
    public ParquetAppendDataStateData(
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            Instant lastKnownTimestamp,
            CompressionCodecName compressionCodecName,
            PVNameToKeyMapping pv2key,
            Configuration configuration,
            PathResolver pathResolver) {
        super(partitionGranularity, rootFolder, desc, lastKnownTimestamp, pv2key, pathResolver);
        this.compressionCodecName = compressionCodecName;
        this.configuration = configuration;
    }

    /**
     * Find the path to the checksum file for the given file.
     *
     * @param currentPVPath The path to the file.
     * @return The path to the checksum file.
     */
    private static Path getCheckSumPath(Path currentPVPath) {
        return Path.of(String.valueOf(currentPVPath.getParent()), "." + currentPVPath.getFileName() + ".crc");
    }

    private void combineFiles(List<Path> inPaths, Path outPath, boolean recompress) throws IOException {
        Path outTempPath = getOutTempPath(outPath);
        if (inPaths.contains(outTempPath)) {
            outTempPath = getOutTempPath(outTempPath);
        }
        List<org.apache.hadoop.fs.Path> rewriteInPaths = new ArrayList<>();
        if (Files.exists(outPath)) {
            if (new ParquetInfo(outPath, this.configuration).getFirstEvent() == null) {
                // if the file is effectively empty, delete it
                Files.delete(outPath);
                Path outCheckSumPath = getCheckSumPath(outPath);
                Files.delete(outCheckSumPath);
                // if this is the only file, just copy it over
                if (inPaths.size() == 1) {
                    Files.copy(inPaths.get(0), outPath);
                    Files.copy(getCheckSumPath(inPaths.get(0)), outCheckSumPath);
                    return;
                }
            } else {
                rewriteInPaths.add(new org.apache.hadoop.fs.Path(outPath.toUri()));
            }
        }
        rewriteInPaths.addAll(inPaths.stream()
                .map(p -> new org.apache.hadoop.fs.Path(p.toUri()))
                .toList());
        Configuration conf = new Configuration();
        RewriteOptions.Builder rewriteOptionsBuilder =
                new RewriteOptions.Builder(conf, rewriteInPaths, new org.apache.hadoop.fs.Path(outTempPath.toUri()));
        if (recompress) {
            rewriteOptionsBuilder = rewriteOptionsBuilder.transform(compressionCodecName);
        }
        RewriteOptions rewriteOptions = rewriteOptionsBuilder.build();
        try {
            ParquetRewriter rewriter = new ParquetRewriter(rewriteOptions);
            rewriter.processBlocks();
            rewriter.close();
        } catch (Exception e) {
            logger.error("Failed to combine files {}", rewriteInPaths, e);
            throw e;
        }
        // Replace the old file with the new file
        Files.move(outTempPath, outPath, StandardCopyOption.REPLACE_EXISTING);
        Files.move(getCheckSumPath(outTempPath), getCheckSumPath(outPath), StandardCopyOption.REPLACE_EXISTING);
    }

    private static Path getOutTempPath(Path outPath) {
        return Path.of(
                outPath.toAbsolutePath().getParent().toString(),
                TEMP_FILE_PREFIX + outPath.getFileName().toString());
    }

    /**
     * If we have an existing file, then this loads a PBInfo, validates the PV name and then updates the appendDataState
     *
     * @param pvName            The PV name
     * @param currentPVFilePath The PV path
     * @throws IOException &emsp;
     */
    public void updateStateBasedOnExistingFile(String pvName, Path currentPVFilePath) throws IOException {
        logger.debug("parquet updateStateBasedOnExistingFile  pv {} pvPath {} ", pvName, currentPVFilePath);

        ParquetInfo info = new ParquetInfo(currentPVFilePath, configuration);
        if (!info.getPVName().equals(pvName))
            throw new IOException("Trying to append data for " + pvName + " to a file " + currentPVFilePath
                    + " that has data for " + info.getPVName());
        this.previousYear = info.getDataYear();
        if (info.getLastEvent() != null) {
            this.lastKnownTimeStamp = info.getLastEvent().getEventTimeStamp();
        } else {
            logger.error(() -> "Cannot determine last known timestamp when updating state for PV " + pvName
                    + " and path " + currentPVFilePath);
        }

        this.writerBuilder = buildWriterExistingFile(currentPVFilePath, info);
        this.previousFilePath = currentPVFilePath;
    }

    /**
     * In cases where we create a new file, this method is used to create an empty file and write out an header.
     *
     * @param pvName The PV name
     * @param pvPath The PV path
     * @param stream The Event stream
     * @throws IOException &emsp;
     */
    protected void createNewFileAndWriteAHeader(String pvName, Path pvPath, EventStream stream) throws IOException {

        if (Files.exists(pvPath)) {
            if (Files.size(pvPath) == 0) {
                Files.delete(pvPath);
            } else {
                throw new IOException("Trying to write a header into a file that exists " + pvPath.toAbsolutePath());
            }
        }
        logger.debug(() -> desc + ": Writing new Parquet file" + pvPath.toAbsolutePath()
                + " for PV " + pvName
                + " for year " + this.currentEventsYear
                + " of type " + stream.getDescription().getArchDBRType()
                + " of PBPayload "
                + stream.getDescription().getArchDBRType().getPBPayloadType());
        var hadoopPath = new org.apache.hadoop.fs.Path(pvPath.toUri());
        var messageClass =
                DBR2PBMessageTypeMapping.getMessageClass(stream.getDescription().getArchDBRType());
        assert messageClass != null;
        this.writerBuilder = EpicsParquetWriter.builder(hadoopPath)
                .withMessage(messageClass)
                .withPVName(pvName)
                .withYear(this.currentEventsYear)
                .withType(stream.getDescription().getArchDBRType())
                .withCompressionCodec(this.compressionCodecName);
        this.previousFilePath = pvPath;
    }

    /**
     * @param context             &emsp;
     * @param pvName              The PV name
     * @param stream              &emsp;
     * @param extension           &emsp;
     * @param extensionToCopyFrom &emsp;
     */
    @Override
    public int partitionBoundaryAwareAppendData(
            BasicContext context, String pvName, EventStream stream, String extension, String extensionToCopyFrom)
            throws IOException {

        try (stream) {
            int eventsAppended = 0;
            for (Event event : stream) {
                Instant ts = event.getEventTimeStamp();
                if (shouldISkipEventBasedOnTimeStamps(event)) continue;

                shouldISwitchPartitions(context, pvName, extension, ts);

                if (this.writerBuilder == null) {
                    preparePartition(
                            pvName, stream, context, extension, extensionToCopyFrom, ts, null, getPathResolver());
                }

                // We check for monotonicity in timestamps again as we had some fresh data from an existing file.
                if (shouldISkipEventBasedOnTimeStamps(event)) continue;

                if (event.getMessage() == null) {
                    logger.error("event {} is null", event);
                    throw new IOException();
                }
                if (this.writer == null) {
                    this.writer = this.writerBuilder.build();
                }
                writer.write(event.getMessage());

                this.previousYear = this.currentEventsYear;
                this.lastKnownTimeStamp = event.getEventTimeStamp();
                eventsAppended++;
            }
            return eventsAppended;
        } catch (Throwable t) {
            logger.error("Exception appending data for PV " + pvName, t);
            throw new IOException(t);
        } finally {
            this.closeStreams();
        }
    }

    public EpicsParquetWriter.Builder<Object> buildWriterExistingFile(Path currentPVPath, ParquetInfo info) {
        logger.debug("parquet buildWriterExistingFile  pvPath {} fileInfo {} ", currentPVPath, info);
        var tempCurrentPVPath = Path.of(
                currentPVPath.toAbsolutePath().getParent().toString(), TEMP_FILE_PREFIX + currentPVPath.getFileName());
        this.tempFile = tempCurrentPVPath;
        var newHadoopPath = new org.apache.hadoop.fs.Path(tempCurrentPVPath.toUri());
        var messageClass = DBR2PBMessageTypeMapping.getMessageClass(info.getType());

        return EpicsParquetWriter.builder(newHadoopPath)
                .withMessage(messageClass)
                .withPVName(info.getPVName())
                .withYear(info.getDataYear())
                .withType(info.getType());
    }

    /**
     * @param pvName              The PV name
     * @param context             The ETL context
     * @param bulkStream          The ETL bulk stream
     * @param extension           &emsp;
     * @param extensionToCopyFrom &emsp;
     */
    @Override
    public boolean bulkAppend(
            String pvName, ETLContext context, ETLBulkStream bulkStream, String extension, String extensionToCopyFrom)
            throws IOException {
        Event firstEvent = checkStream(pvName, context, bulkStream, ETLParquetFilesStream.class);
        if (firstEvent == null) return false;
        ETLParquetFilesStream etlParquetFilesStream = (ETLParquetFilesStream) bulkStream;

        Path pvPath = null;
        if (this.writerBuilder == null) {
            pvPath = preparePartition(
                    pvName,
                    bulkStream,
                    context,
                    extension,
                    extensionToCopyFrom,
                    firstEvent.getEventTimeStamp(),
                    null,
                    getPathResolver());
        }

        // The preparePartition should have created the needed file; so we only append
        assert pvPath != null;

        this.closeStreams();
        combineFiles(
                etlParquetFilesStream.getPaths(),
                pvPath,
                etlParquetFilesStream.getFirstFileInfo().getCompressionCodecName() != this.compressionCodecName);

        try {
            // Update the last known timestamp and the like...
            updateStateBasedOnExistingFile(pvName, pvPath);
        } finally {
            this.closeStreams();
        }
        return true;
    }

    private void combineWithTempFiles() throws IOException {
        combineWithTempFiles(this.previousFilePath);
    }

    @Override
    public void closeStreams() throws IOException {
        logger.debug("close stream with last time stamp {}", this.lastKnownTimeStamp);

        if (this.writerBuilder != null && this.writer == null) {
            this.tempFile = null;
        }
        this.writerBuilder = null;

        if (this.writer != null) {
            try {
                this.writer.close();
                this.writer = null;
            } catch (IOException ignored) {

            }
        }
        combineWithTempFiles();
        this.writerBuilder = null;
    }

    @Override
    public String toString() {
        return "ParquetAppendDataStateData{" + "configuration="
                + configuration + ", writerBuilder="
                + writerBuilder + ", tempFile="
                + tempFile + ", writer="
                + writer + ", rootFolder='"
                + rootFolder + '\'' + ", desc='"
                + desc + '\'' + ", partitionGranularity="
                + partitionGranularity + ", pv2key="
                + pv2key + ", compressionCodecName="
                + compressionCodecName + ", previousFilePath="
                + previousFilePath + ", currentEventsYear="
                + currentEventsYear + ", previousYear="
                + previousYear + ", lastKnownTimeStamp="
                + lastKnownTimeStamp + '}';
    }

    private void combineWithTempFiles(Path currentPVPath) throws IOException {
        if (tempFile != null) {
            logger.debug("parquet combineWithTempFiles  currentPVPath sizes {} tempFiles {} ", currentPVPath, tempFile);

            combineFiles(List.of(tempFile), currentPVPath, false);
            // Delete tempFile
            if (Files.exists(tempFile)) {
                Files.delete(tempFile);
                Files.delete(getCheckSumPath(tempFile));
            }
            tempFile = null;
        }
    }
}
