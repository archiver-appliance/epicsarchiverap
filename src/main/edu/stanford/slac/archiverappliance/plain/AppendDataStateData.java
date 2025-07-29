package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.etl.ETLBulkStream;
import org.epics.archiverappliance.etl.ETLContext;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Companion class to PlainPBStoragePlugin that handles the appending of event streams in a partition-aware fashion.
 * This is used both by the engine and by ETL.
 *
 * @author mshankar
 *
 */
public class AppendDataStateData {
    private static final Logger logger = LogManager.getLogger(AppendDataStateData.class.getName());

    private final PartitionGranularity partitionGranularity;
    private final String rootFolder;

    private OutputStream os = null;
    private final PBCompressionMode compressionMode;
    protected short previousYear = -1;
    protected Instant lastKnownTimeStamp = Instant.ofEpochSecond(0);
    private Instant nextPartitionFirstSecond = Instant.ofEpochSecond(0);
    // These two pieces of information (previousYear and previousEpochSeconds) are from the store using the last known
    // sample when we appending to an existing stream.
    // See the creation and use of the PBFileInfo object below.
    private short currentEventsYear = -1;

    private final String desc;
    private final PVNameToKeyMapping pv2key;
    private String previousFileName = null;
    /**
     * @param partitionGranularity partitionGranularity of the PB plugin.
     * @param rootFolder           RootFolder of the PB plugin
     * @param desc                 Desc for logging purposes
     * @param lastKnownTimestamp   This is probably the most important argument here. This is the last known timestamp
     *                             in this storage. If null, we assume time(0) for the last known timestamp.
     * @param pv2key               PVNameToKeyMapping
     */
    public AppendDataStateData(
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            Instant lastKnownTimestamp,
            PVNameToKeyMapping pv2key,
            PBCompressionMode compressionMode) {
        this.partitionGranularity = partitionGranularity;
        this.rootFolder = rootFolder;
        this.desc = desc;
        this.pv2key = pv2key;
        if (lastKnownTimestamp != null) {
            this.lastKnownTimeStamp = lastKnownTimestamp;
            this.previousYear = TimeUtils.getYear(lastKnownTimestamp);
        }
        this.compressionMode = compressionMode;
    }

    /**
     * Append data into PB files honoring partition boundaries switching into new partitions as we cross the boundary.
     * <ol>
     * <li>We make sure timestamp monotonicity is maintained.</li>
     * <li>We generate clean partitions.</li>
     * </ol>
     * @param context  &emsp;
     * @param pvName The PV name
     * @param stream  &emsp;
     * @param extension   &emsp;
     * @param extensionToCopyFrom &emsp;
     * @return eventsAppended  &emsp;
     */
    public int partitionBoundaryAwareAppendData(
            BasicContext context, String pvName, EventStream stream, String extension, String extensionToCopyFrom)
            throws IOException {
        try (stream) {
            int eventsAppended = 0;
            for (Event event : stream) {
                Instant ts = event.getEventTimeStamp();
                if (shouldISkipEventBasedOnTimeStamps(event)) continue;

                Path pvPath = null;
                shouldISwitchPartitions(context, pvName, extension, ts, this.compressionMode);

                if (this.os == null) {
                    pvPath = preparePartition(
                            pvName, stream, context, extension, extensionToCopyFrom, ts, pvPath, this.compressionMode);
                }

                // We check for monotonicity in timestamps again as we had some fresh data from an existing file.
                if (shouldISkipEventBasedOnTimeStamps(event)) continue;

                // The raw form is already escaped for new lines
                // We can simply write it as is.
                ByteArray val = event.getRawForm();
                this.os.write(val.data, val.off, val.len);
                this.os.write(LineEscaper.NEWLINE_CHAR);

                this.previousYear = this.currentEventsYear;
                this.lastKnownTimeStamp = event.getEventTimeStamp();
                eventsAppended++;
                // logger.debug("Done appending event " + TimeUtils.convertToISO8601String(event.getEventTimeStamp()) +
                // " into " + previousFileName + " of len " + val.len);
            }
            return eventsAppended;
        } catch (Throwable t) {
            logger.error("Exception appending data for PV " + pvName, t);
            throw new IOException(t);
        } finally {
            this.closeStreams();
        }
    }

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
            BasicContext context,
            String pvName,
            String extension,
            Instant ts,
            PBCompressionMode compressionMode)
            throws IOException {

        if (ts.equals(this.nextPartitionFirstSecond) || ts.isAfter(this.nextPartitionFirstSecond)) {
            Path nextPath = PlainPBPathNameUtility.getFileName(
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
                if (this.previousFileName != null) {
                    logger.debug(desc + ": Encountering a change in partitions in the event stream. "
                            + "Closing out " + this.previousFileName
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

    public void closeStreams() {
        // Simply closing the current stream should be good enough for the roll over to work.
        if (this.os != null)
            try {
                this.os.close();
            } catch (Throwable ignored) {
            }
        // Set this to null outside the try/catch so that we are using a new file even if the close fails.
        this.os = null;
    }

    /**
     * Tell appendData if we should skip this event based on the last known event, current year of the destination file
     * etc...
     *
     * @param event &emsp;
     * @return Boolean   &emsp;
     */
    protected boolean shouldISkipEventBasedOnTimeStamps(Event event) {
        Instant timeStamp = event.getEventTimeStamp();
        this.currentEventsYear = TimeUtils.getYear(timeStamp);
        int compare = timeStamp.compareTo(this.lastKnownTimeStamp);
        if (compare <= 0) {
            // Attempt at insisting that the source of this event stream sticks to the contract and gives us ascending
            // times.
            // This takes nanos into account as well.
            logger.debug(desc + ": Skipping data with a timestamp "
                    + TimeUtils.convertToISO8601String(timeStamp)
                    + "older than the previous timestamp "
                    + TimeUtils.convertToISO8601String(this.lastKnownTimeStamp));
            return true;
        }

        if (timeStamp.isBefore(this.lastKnownTimeStamp)) {
            // Attempt at insisting that the source of this event stream sticks to the contract and gives us ascending
            // times.
            logger.debug(desc + ": Skipping data with a timestamp "
                    + TimeUtils.convertToISO8601String(timeStamp)
                    + "older than the previous timestamp "
                    + TimeUtils.convertToISO8601String(this.lastKnownTimeStamp));

            return true;
        }
        if (this.currentEventsYear < this.previousYear) {
            // Same test as above.
            logger.debug("Skipping data from a year " + this.currentEventsYear + "older than the previous year "
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
            PBCompressionMode compressionMode)
            throws IOException {
        if (pvPath == null) {
            pvPath = PlainPBPathNameUtility.getFileName(
                    this.rootFolder,
                    pvName,
                    ts,
                    extension,
                    this.partitionGranularity,
                    true,
                    context.getPaths(),
                    compressionMode,
                    this.pv2key);
        }

        if (!Files.exists(pvPath)) {
            if (extensionToCopyFrom != null && !extensionToCopyFrom.contentEquals("")) {
                // If the file has not been created yet and if we have an extension to copy from
                // We check for the file with the extensionToCopyFrom
                // If that exists, we make a copy of that
                // This is an attempt to not lose data during ETL appends.
                // We make a copy of the original file if it exists, append to the copy and then do an atomic move.
                // Should we should use path's resolve here?
                Path pathToCopyFrom = context.getPaths()
                        .get(pvPath.toAbsolutePath().toString().replace(extension, extensionToCopyFrom));
                if (Files.exists(pathToCopyFrom)) {
                    logger.debug("Making a backup from "
                            + pathToCopyFrom.toAbsolutePath() + " to file "
                            + pvPath.toAbsolutePath() + " when appending data for pv " + pvName);
                    Files.copy(pathToCopyFrom, pvPath);
                    // We still have to create an os so that the logic can continue.
                    updateStateBasedOnExistingFile(pvName, pvPath);

                } else {
                    logger.debug("File to copy from "
                            + pathToCopyFrom.toAbsolutePath() + " does not exist when appending data for pv "
                            + pvName);
                    createNewFileAndWriteAHeader(pvName, pvPath, stream);
                }
            } else {
                logger.debug("File to copy from is not specified and the file " + pvPath.toAbsolutePath()
                        + " does not exist when appending data for pv " + pvName);
                createNewFileAndWriteAHeader(pvName, pvPath, stream);
            }
        } else {
            if (Files.size(pvPath) <= 0) {
                logger.debug("The dest file " + pvPath.toAbsolutePath()
                        + " exists but is 0 bytes long. Writing the header for pv " + pvName);
                createNewFileAndWriteAHeader(pvName, pvPath, stream);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug(this.desc + ": Appending to existing PB file "
                            + pvPath.toAbsolutePath() + " for PV " + pvName + " for year "
                            + this.currentEventsYear);
                }
                updateStateBasedOnExistingFile(pvName, pvPath);
            }
        }
        return pvPath;
    }

    /**
     * If we have an existing file, then this loads a PBInfo, validates the PV name and then updates the
     * appendDataState
     *
     * @param pvName The PV name
     * @param pvPath The PV path
     * @throws IOException &emsp;
     */
    private void updateStateBasedOnExistingFile(String pvName, Path pvPath) throws IOException {
        PBFileInfo info = new PBFileInfo(pvPath);
        if (!info.getPVName().equals(pvName))
            throw new IOException("Trying to append data for " + pvName + " to a file " + pvPath + " that has data for "
                    + info.getPVName());
        this.previousYear = info.getDataYear();
        if (info.getLastEvent() != null) {
            this.lastKnownTimeStamp = info.getLastEvent().getEventTimeStamp();
        } else {
            logger.error("Cannot determine last known timestamp when updating state for PV " + pvName + " and path "
                    + pvPath.toString());
        }
        this.os = new BufferedOutputStream(
                Files.newOutputStream(pvPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND));
        this.previousFileName = pvPath.getFileName().toString();
    }

    /**
     * In cases where we create a new file, this method is used to create an empty file and write out an header.
     *
     * @param pvName The PV name
     * @param pvPath The PV path
     * @param stream The Event stream
     * @throws IOException &emsp;
     */
    private void createNewFileAndWriteAHeader(String pvName, Path pvPath, EventStream stream) throws IOException {
        if (Files.exists(pvPath) && Files.size(pvPath) > 0) {
            throw new IOException("Trying to write a header into a file that exists "
                    + pvPath.toAbsolutePath().toString());
        }
        if (logger.isDebugEnabled())
            logger.debug(
                    desc + ": Writing new PB file" + pvPath.toAbsolutePath().toString()
                            + " for PV " + pvName
                            + " for year " + this.currentEventsYear
                            + " of type " + stream.getDescription().getArchDBRType()
                            + " of PBPayload "
                            + stream.getDescription().getArchDBRType().getPBPayloadType());
        this.os = new BufferedOutputStream(
                Files.newOutputStream(pvPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING));
        byte[] headerBytes = LineEscaper.escapeNewLines(EPICSEvent.PayloadInfo.newBuilder()
                .setPvname(pvName)
                .setType(stream.getDescription().getArchDBRType().getPBPayloadType())
                .setYear(this.currentEventsYear)
                .build()
                .toByteArray());
        this.os.write(headerBytes);
        this.os.write(LineEscaper.NEWLINE_CHAR);
        this.previousFileName = pvPath.getFileName().toString();
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
    public boolean bulkAppend(
            String pvName, ETLContext context, ETLBulkStream bulkStream, String extension, String extensionToCopyFrom)
            throws IOException {
        Event firstEvent = bulkStream.getFirstEvent(context);
        if (this.shouldISkipEventBasedOnTimeStamps(firstEvent)) {
            logger.error(
                    "The bulk append functionality works only if we the first event fits cleanly in the current stream for pv "
                            + pvName + " for stream "
                            + bulkStream.getDescription().getSource());
            return false;
        }

        Path pvPath = null;
        if (this.os == null) {
            pvPath = preparePartition(
                    pvName,
                    bulkStream,
                    context,
                    extension,
                    extensionToCopyFrom,
                    firstEvent.getEventTimeStamp(),
                    pvPath,
                    this.compressionMode);
        }
        this.closeStreams();

        // The preparePartition should have created the needed file; so we only append
        try (ByteChannel destChannel = Files.newByteChannel(pvPath, StandardOpenOption.APPEND);
                ReadableByteChannel srcChannel = bulkStream.getByteChannel(context)) {
            logger.debug("ETL bulk appends for pv " + pvName);
            ByteBuffer buf = ByteBuffer.allocate(1024 * 1024);
            int bytesRead = srcChannel.read(buf);
            while (bytesRead > 0) {
                buf.flip();
                destChannel.write(buf);
                buf.clear();
                bytesRead = srcChannel.read(buf);
            }
        }

        try {
            // Update the last known timestamp and the like...
            updateStateBasedOnExistingFile(pvName, pvPath);
        } finally {
            this.closeStreams();
        }
        return true;
    }
}
