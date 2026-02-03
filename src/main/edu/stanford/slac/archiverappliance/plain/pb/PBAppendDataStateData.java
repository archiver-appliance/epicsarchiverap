package edu.stanford.slac.archiverappliance.plain.pb;

import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import edu.stanford.slac.archiverappliance.plain.EventFileWriter;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import edu.stanford.slac.archiverappliance.plain.PathResolver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.PVNameToKeyMapping;
import org.epics.archiverappliance.etl.ETLBulkStream;
import org.epics.archiverappliance.etl.ETLContext;

import java.io.IOException;
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
    public static final int BULK_BUFFER_INITIAL_CAPACITY = 1024 * 1024;
    private final PBCompressionMode compressionMode;

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
            PBCompressionMode compressionMode,
            PVNameToKeyMapping pv2key,
            PathResolver pathResolver) {
        super(partitionGranularity, rootFolder, desc, lastKnownTimestamp, pv2key, pathResolver);
        this.compressionMode = compressionMode;
    }

    /**
     * Append data into PB files honoring partition boundaries switching into new partitions as we cross the boundary.
     * <ol>
     * <li>We make sure timestamp monotonicity is maintained.</li>
     * <li>We generate clean partitions.</li>
     * </ol>
     *
     * @param pvName              The PV name
     * @param stream              &emsp;
     * @return eventsAppended  &emsp;
     * @throws IOException &emsp;
     */
    @Override
    protected EventFileWriter createNewWriter(String pvName, Path pvPath, EventStream stream) throws IOException {
        return new PBEventFileWriter(pvName, pvPath, stream.getDescription().getArchDBRType(), this.currentEventsYear);
    }

    @Override
    public void updateStateBasedOnExistingFile(String pvName, Path pvPath) throws IOException {
        FileInfo info = new PBFileInfo(pvPath);
        if (!info.getPVName().equals(pvName))
            throw new IOException("Trying to append data for " + pvName
                    + " to a file "
                    + pvPath
                    + " that has data for "
                    + info.getPVName());
        this.previousYear = info.getDataYear();
        if (info.getLastEvent() != null) {
            this.lastKnownTimeStamp = info.getLastEvent().getEventTimeStamp();
        } else {
            logger.error("Cannot determine last known timestamp when updating state for PV " + pvName
                    + " and path "
                    + pvPath.toString());
        }
        this.writer = new PBEventFileWriter(pvName, pvPath, info.getType(), this.previousYear, true);
        this.previousFilePath = pvPath;
    }

    @Override
    public String toString() {
        return ("PBAppendDataStateData{" + ", rootFolder='"
                + rootFolder
                + '\''
                + ", desc='"
                + desc
                + '\''
                + ", partitionGranularity="
                + partitionGranularity
                + ", pv2key="
                + pv2key
                + ", compressionMode="
                + compressionMode
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
        if (this.writer == null) {
            pvPath = preparePartition(
                    pvName,
                    bulkStream,
                    context,
                    extension,
                    extensionToCopyFrom,
                    firstEvent.getEventTimeStamp(),
                    pvPath,
                    getPathResolver());
        }

        this.closeStreams();

        // The preparePartition should have created the needed file; so we only append
        assert pvPath != null;
        try (ByteChannel destChannel = Files.newByteChannel(pvPath, StandardOpenOption.APPEND);
                ReadableByteChannel srcChannel = byteStream.getByteChannel(context)) {
            logger.debug("ETL bulk appends for pv " + pvName);
            ByteBuffer buf = ByteBuffer.allocate(BULK_BUFFER_INITIAL_CAPACITY);
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
