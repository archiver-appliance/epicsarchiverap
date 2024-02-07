package edu.stanford.slac.archiverappliance.plain;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.etl.ETLBulkStream;
import org.epics.archiverappliance.etl.ETLContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

public abstract class AppendDataStateData {
    private static final Logger logger = LogManager.getLogger(AppendDataStateData.class.getName());
    protected final String rootFolder;
    protected final String desc;
    protected final PartitionGranularity partitionGranularity;
    protected final PVNameToKeyMapping pv2key;
    protected final CompressionMode compressionMode;
    protected Path previousFilePath = null;
    protected short currentEventsYear = -1;
    // These two pieces of information (previousYear and previousEpochSeconds) are from the store using the last known
    // sample when we appending to an existing stream.
    // See the creation and use of the PBFileInfo object below.
    protected short previousYear = -1;
    protected Instant lastKnownTimeStamp = Instant.ofEpochSecond(0);
    private Instant nextPartitionFirstSecond = Instant.ofEpochSecond(0);

    /**
     * @param partitionGranularity partitionGranularity of the PB plugin.
     * @param rootFolder           RootFolder of the PB plugin
     * @param desc                 Desc for logging purposes
     * @param lastKnownTimestamp   This is probably the most important argument here. This is the last known timestamp in this storage. If null, we assume time(0) for the last known timestamp.
     * @param pv2key               PVNameToKeyMapping
     */
    public AppendDataStateData(
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            Instant lastKnownTimestamp,
            PVNameToKeyMapping pv2key,
            CompressionMode compressionMode) {
        this.partitionGranularity = partitionGranularity;
        this.rootFolder = rootFolder;
        this.desc = desc;
        this.pv2key = pv2key;
        this.compressionMode = compressionMode;
        if (lastKnownTimestamp != null) {
            this.lastKnownTimeStamp = lastKnownTimestamp;
            this.previousYear = TimeUtils.getYear(lastKnownTimestamp);
        }
    }

    /**
     * Append data into PB files honoring partition boundaries switching into new partitions as we cross the boundary.
     * <ol>
     * <li>We make sure timestamp monotonicity is maintained.</li>
     * <li>We generate clean partitions.</li>
     * </ol>
     *
     * @param context             &emsp;
     * @param pvName              The PV name
     * @param stream              &emsp;
     * @param extension           &emsp;
     * @param extensionToCopyFrom &emsp;
     * @return eventsAppended  &emsp;
     * @throws IOException &emsp;
     */
    public abstract int partitionBoundaryAwareAppendData(
            BasicContext context, String pvName, EventStream stream, String extension, String extensionToCopyFrom)
            throws IOException;

    protected Event checkStream(
            String pvName, ETLContext context, ETLBulkStream bulkStream, Class<? extends ETLBulkStream> streamType)
            throws IOException {
        if (!(streamType.isInstance(bulkStream))) {
            logger.debug("Can't use bulk stream between different file formats "
                    + pvName + " for stream "
                    + bulkStream.getDescription().getSource());
            return null;
        }

        Event firstEvent = bulkStream.getFirstEvent(context);
        if (this.shouldISkipEventBasedOnTimeStamps(firstEvent)) {
            logger.error(
                    "The bulk append functionality works only if we the first event fits cleanly in the current stream for pv "
                            + pvName + " for stream "
                            + bulkStream.getDescription().getSource());
            return null;
        }
        return firstEvent;
    }

    /**
     * Append data in bulk skipping some of the per event checks.
     *
     * @param pvName              The PV name
     * @param context             The ETL context
     * @param bulkStream          The ETL bulk stream
     * @param extension           &emsp;
     * @param extensionToCopyFrom &emsp;
     * @return boolean &emsp;
     * @throws IOException &emsp;
     */
    public abstract boolean bulkAppend(
            String pvName, ETLContext context, ETLBulkStream bulkStream, String extension, String extensionToCopyFrom)
            throws IOException;

    /**
     * Should we switch to a new partition? If so, return the new partition, else return the current partition.
     *
     * @param context   &emsp;
     * @param pvName    The PV name
     * @param extension &emsp;
     * @param ts        The epoch seconds
     * @throws IOException &emsp;
     */
    protected void shouldISwitchPartitions(
            BasicContext context, String pvName, String extension, Instant ts, CompressionMode compressionMode)
            throws IOException {

        if (ts.equals(this.nextPartitionFirstSecond) || ts.isAfter(this.nextPartitionFirstSecond)) {
            Path nextPath = PathNameUtility.getFileName(
                    this.rootFolder,
                    pvName,
                    ts,
                    extension,
                    this.partitionGranularity,
                    true,
                    context.getPaths(),
                    compressionMode,
                    this.pv2key);
            this.nextPartitionFirstSecond = TimeUtils.getNextPartitionFirstSecond(ts, this.partitionGranularity);
            if (logger.isDebugEnabled()) {
                if (this.previousFilePath != null) {
                    logger.debug(desc + ": Encountering a change in partitions in the event stream. "
                            + "Closing out " + this.previousFilePath
                            + " to make way for " + nextPath
                            + " Next partition is to be switched at "
                            + TimeUtils.convertToISO8601String(this.nextPartitionFirstSecond));
                } else {
                    logger.debug(
                            desc + ": New partition into file " + nextPath + " Next partition is to be switched at "
                                    + TimeUtils.convertToISO8601String(this.nextPartitionFirstSecond));
                }
            }
            this.closeStreams();
        }
    }

    /**
     * Tell appendData if we should skip this event based on the last known event,
     * current year of the destination file etc...
     *
     * @param event &emsp;
     * @return Boolean   &emsp;
     */
    protected boolean shouldISkipEventBasedOnTimeStamps(Event event) {
        Instant timeStamp = event.getEventTimeStamp();
        this.currentEventsYear = TimeUtils.getYear(timeStamp);
        Instant currentTimeStamp = event.getEventTimeStamp();
        int compare = currentTimeStamp.compareTo(this.lastKnownTimeStamp);
        if (compare <= 0) {
            // Attempt at insisting that the source of this event stream sticks to the contract and gives us ascending
            // times.
            // This takes nanos into account as well.
            logger.debug(desc + ": Skipping data with a timestamp "
                    + TimeUtils.convertToISO8601String(timeStamp)
                    + "older than the previous timstamp "
                    + TimeUtils.convertToISO8601String(this.lastKnownTimeStamp));
            return true;
        }

        if (timeStamp.isBefore(this.lastKnownTimeStamp)) {
            // Attempt at insisting that the source of this event stream sticks to the contract and gives us ascending
            // times.
            logger.info(desc + ": Skipping data with a timestamp "
                    + TimeUtils.convertToISO8601String(timeStamp)
                    + "older than the previous timstamp "
                    + TimeUtils.convertToISO8601String(this.lastKnownTimeStamp));

            return true;
        }
        if (this.currentEventsYear < this.previousYear) {
            // Same test as above.
            logger.info("Skipping data from a year " + this.currentEventsYear + "older than the previous year "
                    + this.previousYear);
            return true;
        }

        return false;
    }

    /**
     * Prepare a new partition.
     *
     * @param pvName              The PV name
     * @param stream              &emsp;
     * @param context             &emsp;
     * @param extension           &emsp;
     * @param extensionToCopyFrom &emsp;
     * @param ts                  The epoch seconds
     * @param pvPath              &emsp;
     * @return pvPath  &emsp;
     * @throws IOException &emsp;
     */
    protected Path preparePartition(
            String pvName,
            EventStream stream,
            BasicContext context,
            String extension,
            String extensionToCopyFrom,
            Instant ts,
            Path pvPath,
            CompressionMode compressionMode)
            throws IOException {
        Path preparePath;
        if (pvPath == null) {
            preparePath = PathNameUtility.getFileName(
                    this.rootFolder,
                    pvName,
                    ts,
                    extension,
                    this.partitionGranularity,
                    true,
                    context.getPaths(),
                    compressionMode,
                    this.pv2key);
        } else {
            preparePath = pvPath;
        }

        if (!Files.exists(preparePath)) {
            if (extensionToCopyFrom != null && !extensionToCopyFrom.contentEquals("")) {
                // If the file has not been created yet and if we have an extension to copy from
                // We check for the file with the extensionToCopyFrom
                // If that exists, we make a copy of that
                // This is an attempt to not lose data during ETL appends.
                // We make a copy of the original file if it exists, append to the copy and then do an atomic move.
                // Should we should use path's resolve here?
                Path pathToCopyFrom = context.getPaths()
                        .get(preparePath.toAbsolutePath().toString().replace(extension, extensionToCopyFrom));
                if (Files.exists(pathToCopyFrom)) {
                    logger.debug("Making a backup from "
                            + pathToCopyFrom.toAbsolutePath() + " to file "
                            + preparePath.toAbsolutePath() + " when appending data for pv " + pvName);
                    Files.copy(pathToCopyFrom, preparePath);
                    // We still have to create an os so that the logic can continue.
                    updateStateBasedOnExistingFile(pvName, preparePath);

                } else {
                    logger.debug("File to copy from "
                            + pathToCopyFrom.toAbsolutePath() + " does not exist when appending data for pv "
                            + pvName);
                    createNewFileAndWriteAHeader(pvName, preparePath, stream);
                }
            } else {
                logger.debug("File to copy from is not specified and the file " + preparePath.toAbsolutePath()
                        + " does not exist when appending data for pv " + pvName);
                createNewFileAndWriteAHeader(pvName, preparePath, stream);
            }
        } else {
            if (Files.size(preparePath) <= 0) {
                logger.debug("The dest file " + preparePath.toAbsolutePath()
                        + " exists but is 0 bytes long. Writing the header for pv " + pvName);
                createNewFileAndWriteAHeader(pvName, preparePath, stream);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug(this.desc + ": Appending to existing PB file "
                            + preparePath.toAbsolutePath() + " for PV " + pvName + " for year "
                            + this.currentEventsYear);
                }
                updateStateBasedOnExistingFile(pvName, preparePath);
            }
        }
        return preparePath;
    }

    protected abstract void createNewFileAndWriteAHeader(String pvName, Path pvPath, EventStream stream)
            throws IOException;

    protected abstract void updateStateBasedOnExistingFile(String pvName, Path pvPath) throws IOException;

    public abstract void closeStreams() throws IOException;
}
