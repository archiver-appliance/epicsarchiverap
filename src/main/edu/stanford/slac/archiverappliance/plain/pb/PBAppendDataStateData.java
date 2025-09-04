package edu.stanford.slac.archiverappliance.plain.pb;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import edu.stanford.slac.archiverappliance.plain.AppendDataStateData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.common.PartitionGranularity;
import org.epics.archiverappliance.config.PVNameToKeyMapping;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

public class PBAppendDataStateData extends AppendDataStateData {
    private static final Logger logger = LogManager.getLogger(PBAppendDataStateData.class.getName());

    /**
     * @param partitionGranularity partitionGranularity of the PB plugin.
     * @param rootFolder           RootFolder of the PB plugin
     * @param desc                 Desc for logging purposes
     * @param lastKnownTimestamp   This is probably the most important argument here. This is the last known timestamp
     *                             in this storage. If null, we assume time(0) for the last known timestamp.
     * @param pv2key               PVNameToKeyMapping
     * @param compressionMode
     */
    public PBAppendDataStateData(
            PartitionGranularity partitionGranularity,
            String rootFolder,
            String desc,
            Instant lastKnownTimestamp,
            PVNameToKeyMapping pv2key,
            PBCompressionMode compressionMode) {
        super(partitionGranularity, rootFolder, desc, lastKnownTimestamp, pv2key, compressionMode);
    }

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

    @Override
    protected void updateStateBasedOnExistingFile(String pvName, Path pvPath) throws IOException {
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

    @Override
    protected void createNewFileAndWriteAHeader(String pvName, Path pvPath, EventStream stream) throws IOException {
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
}
