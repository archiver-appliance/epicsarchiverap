package edu.stanford.slac.archiverappliance.plain.parquet;

import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.EventFileWriter;
import edu.stanford.slac.archiverappliance.plain.PathResolver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.rewrite.ParquetRewriter;
import org.apache.parquet.hadoop.rewrite.RewriteOptions;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.LocalOutputFile;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
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
 * A stateful class that manages the process of writing and appending EPICS event data to Parquet files.
 * <p>
 * This class extends {@link AppendDataStateData} to provide Parquet-specific logic. It handles
 * file partitioning, writing new events, and merging data.
 * <p>
 * Key behaviors:
 * <ul>
 *     <li><b>Incremental Appends:</b> When appending new events to an existing file, this class writes
 *     the new data to a temporary file. When the stream is closed, this temporary file is efficiently
 *     merged with the original data file.</li>
 *     <li><b>Bulk Appends (ETL):</b> For ETL operations, it uses an optimized path that leverages
 *     {@link ParquetRewriter} to merge multiple source Parquet files directly into the destination
 *     file without full deserialization.</li>
 * </ul>
 *
 */
public class ParquetAppendDataStateData extends AppendDataStateData {

    private static final Logger logger = LogManager.getLogger(ParquetAppendDataStateData.class.getName());
    private static final String TEMP_FILE_PREFIX = "~TempFile~";
    private final ParquetReadOptions readOptions;
    private final CompressionCodecName compressionCodecName;
    private Path tempFile;

    /**
     * Constructs a new state manager for appending data to Parquet files.
     *
     * @param partitionGranularity The granularity for partitioning data files (e.g., YEARLY).
     * @param rootFolder           The root directory for storage.
     * @param desc                 A description for logging purposes.
     * @param lastKnownTimestamp   The timestamp of the last known event in this storage partition.
     *                             Used to prevent writing out-of-order data.
     * @param compressionCodecName The compression codec to use for writing new files.
     * @param pv2key               The mapping from PV name to storage key.
     * @param readOptions          Configuration for reading Parquet files.
     * @param pathResolver         The resolver for determining file paths.
     */
    public ParquetAppendDataStateData(
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            Instant lastKnownTimestamp,
            CompressionCodecName compressionCodecName,
            PVNameToKeyMapping pv2key,
            ParquetReadOptions readOptions,
            PathResolver pathResolver) {
        super(partitionGranularity, rootFolder, desc, lastKnownTimestamp, pv2key, pathResolver);
        this.compressionCodecName = compressionCodecName;
        this.readOptions = readOptions;
    }

    /**
     * Merges a list of input Parquet files into a single output file using {@link ParquetRewriter}.
     * <p>
     * This method performs a low-level, efficient merge of Parquet row groups. The operation is atomic:
     * it writes to a temporary file first and then replaces the original output file upon success.
     *
     * @param inPaths    A list of source Parquet files to merge.
     * @param outPath    The destination file. If it exists, it will be included as the first file in the merge.
     * @param recompress If true, the merged output will be written with the handler's configured compression codec.
     *                   Otherwise, the original compression of the source files is preserved.
     * @throws IOException if an I/O error occurs during the rewrite process.
     */
    private void combineFiles(List<Path> inPaths, Path outPath, boolean recompress) throws IOException {
        Path outTempPath = getOutTempPath(outPath);
        if (inPaths.contains(outTempPath)) {
            outTempPath = getOutTempPath(outTempPath);
        }
        List<InputFile> rewriteInPaths = new ArrayList<>();
        if (Files.exists(outPath)) {
            if (new ParquetInfo(outPath, this.readOptions).getFirstEvent() == null) {
                // if the file is effectively empty, delete it
                Files.delete(outPath);
                // if this is the only file, just copy it over
                if (inPaths.size() == 1) {
                    Files.copy(inPaths.getFirst(), outPath);
                    return;
                }
            } else {
                rewriteInPaths.add(new LocalInputFile(outPath));
            }
        }
        rewriteInPaths.addAll(inPaths.stream().map(LocalInputFile::new).toList());
        ParquetConfiguration conf = new PlainParquetConfiguration();
        RewriteOptions.Builder rewriteOptionsBuilder =
                new RewriteOptions.Builder(conf, rewriteInPaths, new LocalOutputFile(outTempPath));
        if (recompress) {
            rewriteOptionsBuilder = rewriteOptionsBuilder.transform(compressionCodecName);
        }
        RewriteOptions rewriteOptions = rewriteOptionsBuilder.build();
        try (ParquetRewriter rewriter = new ParquetRewriter(rewriteOptions)) {
            rewriter.processBlocks();
        } catch (Exception e) {
            logger.error("Failed to combine files {}", rewriteInPaths, e);
            throw e;
        }
        // Replace the old file with the new file
        Files.move(outTempPath, outPath, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Generates a temporary file path based on a given output path.
     *
     * @param outPath The final destination path.
     * @return A temporary path in the same directory, prefixed with '~'.
     */
    private static Path getOutTempPath(Path outPath) {
        return Path.of(
                outPath.toAbsolutePath().getParent().toString(),
                TEMP_FILE_PREFIX + outPath.getFileName().toString());
    }

    /**
     * Initializes the writer state by reading metadata from an existing Parquet file.
     * This is called when appending data to a file that already contains events.
     *
     * @param pvName            The name of the PV.
     * @param currentPVFilePath The path to the existing Parquet file.
     * @throws IOException if the PV name in the file does not match or an I/O error occurs.
     */
    public void updateStateBasedOnExistingFile(String pvName, Path currentPVFilePath) throws IOException {
        logger.debug("parquet updateStateBasedOnExistingFile  pv {} pvPath {} ", pvName, currentPVFilePath);

        ParquetInfo info = new ParquetInfo(currentPVFilePath, readOptions);
        if (!info.getPVName().equals(pvName))
            throw new IOException("Trying to append data for " + pvName
                    + " to a file "
                    + currentPVFilePath
                    + " that has data for "
                    + info.getPVName());
        this.previousYear = info.getDataYear();
        if (info.getLastEvent() != null) {
            this.lastKnownTimeStamp = info.getLastEvent().getEventTimeStamp();
        } else {
            logger.error(() -> "Cannot determine last known timestamp when updating state for PV " + pvName
                    + " and path "
                    + currentPVFilePath);
        }

        var tempCurrentPVPath = Path.of(
                currentPVFilePath.toAbsolutePath().getParent().toString(),
                TEMP_FILE_PREFIX + currentPVFilePath.getFileName());
        this.tempFile = tempCurrentPVPath;

        this.writer = new ParquetEventFileWriter(
                info.getPVName(), tempCurrentPVPath, info.getType(), info.getDataYear(), compressionCodecName);
        this.previousFilePath = currentPVFilePath;
    }

    @Override
    protected EventFileWriter createNewWriter(String pvName, Path pvPath, EventStream stream) throws IOException {
        return new ParquetEventFileWriter(
                pvName,
                pvPath,
                stream.getDescription().getArchDBRType(),
                this.currentEventsYear,
                this.compressionCodecName);
    }

    /**
     * Appends events from a stream, handling partition boundaries.
     * This is the primary method for writing individual events.
     *
     * @return The number of events successfully appended.
     * @throws IOException if a critical error occurs during writing.
     */

    /**
     * Performs a bulk append optimized for ETL processes.
     * <p>
     * This method expects an {@link ETLParquetFilesStream} and uses {@link #combineFiles(List, Path, boolean)}
     * to merge the source files directly, bypassing event-by-event processing.
     *
     * @return {@code true} if the append was successful.
     * @throws IOException if the stream is not of the expected type or an I/O error occurs.
     */
    @Override
    public boolean bulkAppend(
            String pvName, ETLContext context, ETLBulkStream bulkStream, String extension, String extensionToCopyFrom)
            throws IOException {
        Event firstEvent = checkStream(pvName, context, bulkStream, ETLParquetFilesStream.class);
        if (firstEvent == null) return false;
        ETLParquetFilesStream etlParquetFilesStream = (ETLParquetFilesStream) bulkStream;

        Path pvPath = null;
        if (this.writer == null) {
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

    /**
     * Merges the temporary append file (if it exists) with its corresponding main data file.
     *
     * @throws IOException if an I/O error occurs during the merge.
     */
    private void combineWithTempFiles() throws IOException {
        combineWithTempFiles(this.previousFilePath);
    }

    /**
     * Closes any open writers and merges temporary files.
     * <p>
     * This method is critical for ensuring data is committed to its final destination. It closes
     * the {@link ParquetWriter} (flushing its buffers) and then calls {@link #combineWithTempFiles()}
     * to merge the newly written data into the main partition file.
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
    public void closeStreams() throws IOException {
        logger.debug("close stream with last time stamp {}", this.lastKnownTimeStamp);
        super.closeStreams();
        combineWithTempFiles();
    }

    @Override
    public String toString() {
        return ("ParquetAppendDataStateData{" + "readOptions="
                + readOptions
                + ", writer="
                + writer
                + ", tempFile="
                + tempFile
                + ", writer="
                + writer
                + ", rootFolder='"
                + rootFolder
                + '\''
                + ", desc='"
                + desc
                + '\''
                + ", partitionGranularity="
                + partitionGranularity
                + ", pv2key="
                + pv2key
                + ", compressionCodecName="
                + compressionCodecName
                + ", previousFilePath="
                + previousFilePath
                + ", currentEventsYear="
                + currentEventsYear
                + ", previousYear="
                + previousYear
                + ", lastKnownTimeStamp="
                + lastKnownTimeStamp
                + '}');
    }

    /**
     * Merges the temporary append file (if it exists) with the specified main data file.
     *
     * @param currentPVPath The path to the main data file.
     * @throws IOException if an I/O error occurs during the merge or cleanup.
     */
    private void combineWithTempFiles(Path currentPVPath) throws IOException {
        if (tempFile != null && Files.exists(tempFile)) {
            logger.debug("parquet combineWithTempFiles  currentPVPath sizes {} tempFiles {} ", currentPVPath, tempFile);

            combineFiles(List.of(tempFile), currentPVPath, false);
            // Delete tempFile
            if (Files.exists(tempFile)) {
                Files.delete(tempFile);
            }
            tempFile = null;
        } else {
            tempFile = null;
        }
    }
}
