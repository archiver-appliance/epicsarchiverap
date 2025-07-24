/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain.pb;

import edu.stanford.slac.archiverappliance.PB.search.FileEventStreamSearch;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;

/**
 * An eventstream that spans multiple PB files.
 * You can only get one iterator out of this event stream. This condition is also checked for.
 * This is typically used with/after PlainPBFileNameUtility.getFilesWithData
 * @author mshankar
 *
 */
public class MultiFilePBEventStream implements EventStream, RemotableOverRaw {
    private static final Logger logger = LogManager.getLogger(MultiFilePBEventStream.class);
    private final String pvName;
    private final ArchDBRTypes type;
    private final RemotableEventStreamDesc desc;
    private LineByteStreamCreator[] istreams = null;
    private MultiFilePBEventStreamIterator theIterator = null;

    public MultiFilePBEventStream(Path[] paths, String pvName, ArchDBRTypes dbrtype, Instant startTime, Instant endTime)
            throws IOException {
        this.pvName = pvName;
        this.type = dbrtype;

        YearSecondTimestamp startYTS = TimeUtils.convertToYearSecondTimestamp(startTime);
        YearSecondTimestamp endYTS = TimeUtils.convertToYearSecondTimestamp(endTime);

        // We need at least two files for this event stream to work correctly.
        assert (paths.length > 1);
        PBFileInfo pbinfo = new PBFileInfo(paths[0]);
        this.desc = new RemotableEventStreamDesc(this.pvName, pbinfo.getInfo());

        istreams = new LineByteStreamCreator[paths.length];
        for (int i = 0; i < paths.length; i++) {
            Path path = paths[i];
            pbinfo = new PBFileInfo(path);
            try {
                if (i == 0) {
                    if (pbinfo.getDataYear() == startYTS.getYear()) {
                        logger.debug("Looking for start position in file " + path.toAbsolutePath());
                        FileEventStreamSearch bsstart =
                                new FileEventStreamSearch(path, pbinfo.getPositionOfFirstSample());
                        boolean startfound = bsstart.seekToTime(dbrtype, startYTS);
                        long startPosition = 0;
                        if (startfound) {
                            startPosition = bsstart.getFoundPosition();
                            logger.debug("Found start position " + startPosition + " in file " + path.toAbsolutePath());
                            LineByteStream lis = new LineByteStream(path, startPosition);
                            if (startPosition == 0L) {
                                lis.readLine();
                            } else {
                                lis.seekToFirstNewLine();
                            }
                            istreams[i] = new LineByteStreamCreator(lis, pvName, type);
                        } else {
                            logger.warn("Did not find start position in file "
                                    + path.toAbsolutePath().toString() + " for time "
                                    + TimeUtils.convertToISO8601String(startTime) + " using entire file");
                            istreams[i] = new LineByteStreamCreator(path, pvName, type);
                        }
                    } else {
                        logger.debug("Start year in file " + pbinfo.getDataYear()
                                + " is not the same as the start time " + startYTS.getYear() + " using entire file");
                        istreams[i] = new LineByteStreamCreator(path, pvName, type);
                    }
                } else if (i == (paths.length - 1)) {
                    if (pbinfo.getDataYear() == endYTS.getYear()) {
                        FileEventStreamSearch bsend = new FileEventStreamSearch(path, pbinfo.positionOfFirstSample);
                        boolean endfound = bsend.seekToTime(dbrtype, endYTS);
                        long endPosition = Files.size(path);
                        if (endfound) {
                            endPosition = bsend.getFoundPosition();
                            logger.debug("Found end position " + endPosition + " in file "
                                    + path.toAbsolutePath().toString());
                            LineByteStream lis = new LineByteStream(path, 0, endPosition);
                            PBFileInfo.checkPayloadInfo(lis, pvName, type);
                            istreams[i] = new LineByteStreamCreator(lis, pvName, type);
                        } else {
                            logger.warn("Did not find end position in file "
                                    + path.toAbsolutePath().toString() + " for time "
                                    + TimeUtils.convertToISO8601String(endTime) + " using entire file");
                            istreams[i] = new LineByteStreamCreator(path, pvName, type);
                        }
                    } else {
                        logger.debug("End year in file " + pbinfo.getDataYear() + " is not the same as the end time "
                                + endYTS.getYear() + " using entire file");
                        istreams[i] = new LineByteStreamCreator(path, pvName, type);
                    }
                } else {
                    // Use whole file for chunks in the middle.
                    logger.debug("Using all the data from file " + path.toAbsolutePath());
                    istreams[i] = new LineByteStreamCreator(path, pvName, type);
                }
            } catch (IOException ex) {
                throw new IOException(
                        "Exception processing file " + path.toAbsolutePath().toString(), ex);
            }
        }
    }

    @Override
    public Iterator<Event> iterator() {
        if (theIterator != null) {
            logger.error(
                    "We can only support one iterator per MultiFilePBEventStream. This one already has an iterator created.");
            return null;
        }
        try {
            theIterator = new MultiFilePBEventStreamIterator(istreams, this.pvName, this.desc.getYear(), this.type);
            return theIterator;
        } catch (IOException ex) {
            logger.error("Exception creating iterator", ex);
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        for (LineByteStreamCreator lis : istreams) {
            lis.safeClose();
        }
    }

    @Override
    public RemotableEventStreamDesc getDescription() {
        return desc;
    }
}
