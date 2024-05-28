/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain.pb;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadInfo;
import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.data.PBParseException;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import edu.stanford.slac.archiverappliance.plain.FileInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Gets some information about PB files.
 * Important information includes the first and last event.
 *
 * @author mshankar
 *
 */
public class PBFileInfo extends FileInfo {
    private static final Logger logger = LogManager.getLogger(PBFileInfo.class);
    PayloadInfo info;
    long positionOfFirstSample = 0L;
    long positionOfLastSample = 0;

    @Override
    public String toString() {
        return "PBFileInfo{" + "info="
                + info + ", positionOfFirstSample="
                + positionOfFirstSample + ", positionOfLastSample="
                + positionOfLastSample + ", firstEvent="
                + firstEvent + ", lastEvent="
                + lastEvent + '}';
    }

    public PBFileInfo(Path path) throws IOException {
        this(path, true);
    }

    public PBFileInfo(Path path, boolean lookupLastEvent) throws IOException {
        super();

        try (LineByteStream lis = new LineByteStream(path)) {
            byte[] payloadLine = LineEscaper.unescapeNewLines(lis.readLine());
            info = PayloadInfo.parseFrom(payloadLine);
            logger.debug("PayloadInfo PVName: " + info.getPvname() + " is of type "
                    + info.getType().name() + " and data is for the year " + info.getYear());
            positionOfFirstSample = lis.getCurrentPosition();
            // This is not strictly correct; but this will be adjusted below.
            positionOfLastSample = Files.size(path);

            ArchDBRTypes type = ArchDBRTypes.valueOf(info.getType());
            Constructor<? extends DBRTimeEvent> unmarshallingConstructor =
                    DBR2PBTypeMapping.getPBClassFor(type).getUnmarshallingFromByteArrayConstructor();
            // Let's read the first line
            lis.seekToFirstNewLine();
            byte[] firstLine = lis.readLine();
            if (firstLine != null) {
                firstEvent =
                        (DBRTimeEvent) unmarshallingConstructor.newInstance(getDataYear(), new ByteArray(firstLine));
                if (lookupLastEvent) {
                    // If we do not have a first line, we probably do not have a last line
                    this.lookupLastEvent(path, lis, unmarshallingConstructor);
                }
            } else {
                logger.debug("File " + path.toAbsolutePath().toString() + " does not seem to have any first line?");
            }
        } catch (Exception e) {
            logger.warn(
                    "Exception determing header information from file "
                            + path.toAbsolutePath().toString(),
                    e);
            throw new IOException(e);
        }
    }

    public String getPVName() {
        return info.getPvname();
    }

    public short getDataYear() {
        return (short) info.getYear();
    }

    public ArchDBRTypes getType() {
        return ArchDBRTypes.valueOf(info.getType());
    }

    public PayloadInfo getInfo() {
        return info;
    }

    public DBRTimeEvent getFirstEvent() {
        return firstEvent;
    }

    public DBRTimeEvent getLastEvent() {
        return lastEvent;
    }

    public long getPositionOfFirstSample() {
        return positionOfFirstSample;
    }

    public long getPositionOfLastSample() {
        return positionOfLastSample;
    }

    /**
     * Checks the payload info and makes sure we are using appropriate files.
     * This assumes that the lis is positioned at the start and subsequently positions the lis just past the first line.
     * So if we need to position the lis elsewhere, the caller needs to do that manually after this call.
     * @param lis The line bytes stream
     * @param pvName  The PV name
     * @param type  Enum ArchDBRTypes
     * @throws IOException  &emsp;
     */
    public static void checkPayloadInfo(LineByteStream lis, String pvName, ArchDBRTypes type) throws IOException {
        byte[] payloadLine = LineEscaper.unescapeNewLines(lis.readLine());
        PayloadInfo info = PayloadInfo.parseFrom(payloadLine);
        if (!pvName.equals(info.getPvname())) {
            logger.error("File " + lis.getAbsolutePath() + " is being used to read data for pv " + pvName
                    + " but it actually contains data for pv " + info.getPvname());
        }
        if (!type.equals(ArchDBRTypes.valueOf(info.getType()))) {
            throw new IOException("File " + lis.getAbsolutePath() + " contains "
                    + ArchDBRTypes.valueOf(info.getType()).toString() + " we are expecting " + type.toString());
        }
    }

    private void lookupLastEvent(
            Path path, LineByteStream lis, Constructor<? extends DBRTimeEvent> unmarshallingConstructor)
            throws Exception {
        // If we do not have a first line, we probably do not have a last line
        lis.seekToBeforeLastLine();
        long posn = lis.getCurrentPosition();
        int lineTries = 0;
        byte[] lastLine = lis.readLine();
        while (lastLine == null && posn > 0 && lineTries < 1000) {
            lis.seekToBeforePreviousLine(posn - 2);
            posn = lis.getCurrentPosition();
            lastLine = lis.readLine();
            lineTries++;
        }

        int tries = 0;
        // Potential infinite loop here; we'll try about 1000 times
        while (lastEvent == null && lastLine != null && tries < 1000) {
            try {
                lastEvent = (DBRTimeEvent) unmarshallingConstructor.newInstance(getDataYear(), new ByteArray(lastLine));
                lastEvent.getEventTimeStamp();
                positionOfLastSample = posn;
                return;
            } catch (PBParseException ex) {
                logger.warn(
                        path.toString()
                                + " seems to have some data corruption at the end of the file; moving onto the previous line",
                        ex);
                lastEvent = null;
                lis.seekToBeforePreviousLine(posn - 2);
                posn = lis.getCurrentPosition();
                lineTries = 0;
                lastLine = lis.readLine();
                while (lastLine == null && posn > 0 && lineTries < 1000) {
                    lis.seekToBeforePreviousLine(posn - 2);
                    posn = lis.getCurrentPosition();
                    lastLine = lis.readLine();
                    lineTries++;
                }
            }
            tries++;
        }

        logger.debug("File " + path.toAbsolutePath().toString() + " does not seem to have any last line?");
        lastEvent = null;
    }
}
