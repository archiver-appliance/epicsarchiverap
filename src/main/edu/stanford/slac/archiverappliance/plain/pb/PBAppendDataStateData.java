package edu.stanford.slac.archiverappliance.plain.pb;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.CompressionMode;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
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
 * Companion class to PlainStoragePlugin that handles the appending of event streams in a partition-aware fashion.
 * This is used both by the engine and by ETL.
 *
 * @author mshankar
 */
public class PBAppendDataStateData extends AppendDataStateData {
    private static final Logger logger = LogManager.getLogger(PBAppendDataStateData.class.getName());
    private OutputStream os = null;

    /**
     * @param partitionGranularity partitionGranularity of the PB plugin.
     * @param rootFolder           RootFolder of the PB plugin
     * @param desc                 Desc for logging purposes
     * @param lastKnownTimestamp   This is probably the most important argument here. This is the last known timestamp in this storage. If null, we assume time(0) for the last known timestamp.
     * @param compressionMode      CompressionMode
     * @param pv2key               PVNameToKeyMapping
     */
    public PBAppendDataStateData(
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            Instant lastKnownTimestamp,
            CompressionMode compressionMode,
            PVNameToKeyMapping pv2key) {
        super(partitionGranularity, rootFolder, desc, lastKnownTimestamp, pv2key, compressionMode);
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
    @Override
    public int partitionBoundaryAwareAppendData(
            BasicContext context, String pvName, EventStream stream, String extension, String extensionToCopyFrom)
            throws IOException {
        try (stream) {
            int eventsAppended = 0;
            for (Event event : stream) {
                Instant ts = event.getEventTimeStamp();
                if (shouldISkipEventBasedOnTimeStamps(event)) continue;

                Path pvPath = null;
                shouldISwitchPartitions(context, pvName, extension, ts, compressionMode);

                if (this.os == null) {
                    preparePartition(
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
            }
            return eventsAppended;
        } catch (Throwable t) {
            logger.error("Exception appending data for PV " + pvName, t);
            throw new IOException(t);
        } finally {
            this.closeStreams();
        }
    }

    @Override
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
     * If we have an existing file, then this loads a PBInfo, validates the PV name and then updates the appendDataState
     *
     * @param pvName The PV name
     * @param pvPath The PV path
     * @throws IOException &emsp;
     */
    public void updateStateBasedOnExistingFile(String pvName, Path pvPath) throws IOException {
        FileInfo info = new PBFileInfo(pvPath);
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
        this.previousFilePath = pvPath;
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
        if (Files.exists(pvPath) && Files.size(pvPath) > 0) {
            throw new IOException("Trying to write a header into a file that exists " + pvPath.toAbsolutePath());
        }
        if (logger.isDebugEnabled())
            logger.debug(desc + ": Writing new PB file" + pvPath.toAbsolutePath()
                    + " for PV " + pvName
                    + " for year " + this.currentEventsYear
                    + " of type " + stream.getDescription().getArchDBRType()
                    + " of PBPayload "
                    + stream.getDescription().getArchDBRType().getPBPayloadType());
        OutputStream outputStream =
                Files.newOutputStream(pvPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        this.os = new BufferedOutputStream(outputStream);
        byte[] headerBytes = LineEscaper.escapeNewLines(PayloadInfo.newBuilder()
                .setPvname(pvName)
                .setType(stream.getDescription().getArchDBRType().getPBPayloadType())
                .setYear(this.currentEventsYear)
                .build()
                .toByteArray());
        this.os.write(headerBytes);
        this.os.write(LineEscaper.NEWLINE_CHAR);
        this.previousFilePath = pvPath;
    }

    @Override
    public String toString() {
        return "PBAppendDataStateData{" + "os="
                + os + ", rootFolder='"
                + rootFolder + '\'' + ", desc='"
                + desc + '\'' + ", partitionGranularity="
                + partitionGranularity + ", pv2key="
                + pv2key + ", compressionMode="
                + compressionMode + ", previousFilePath="
                + previousFilePath + ", currentEventsYear="
                + currentEventsYear + ", previousYear="
                + previousYear + ", lastKnownTimeStamp="
                + lastKnownTimeStamp + '}';
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
    @Override
    public boolean bulkAppend(
            String pvName, ETLContext context, ETLBulkStream bulkStream, String extension, String extensionToCopyFrom)
            throws IOException {
        Event firstEvent = checkStream(pvName, context, bulkStream, ETLPBByteStream.class);
        if (firstEvent == null) return false;

        ETLPBByteStream byteStream = (ETLPBByteStream) bulkStream;
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
        assert pvPath != null;
        try (ByteChannel destChannel = Files.newByteChannel(pvPath, StandardOpenOption.APPEND);
                ReadableByteChannel srcChannel = byteStream.getByteChannel(context)) {
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
