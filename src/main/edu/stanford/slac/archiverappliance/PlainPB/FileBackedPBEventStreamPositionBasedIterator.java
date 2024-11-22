/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Path;

/**
 * An iterator for a FileBackedPBEventStream.
 * @author mshankar
 *
 */
public class FileBackedPBEventStreamPositionBasedIterator implements FileBackedPBEventStreamIterator {
    private static final Logger logger =
            LogManager.getLogger(FileBackedPBEventStreamPositionBasedIterator.class.getName());
    private short year = 0;
    private LineByteStream lbs = null;
    private final ByteArray nextLine = new ByteArray(LineByteStream.MAX_LINE_SIZE);
    // Whether the line already in nextLine has been used by the iterator
    private boolean lineUsed = false;
    private final Constructor<? extends DBRTimeEvent> unmarshallingConstructor;

    public FileBackedPBEventStreamPositionBasedIterator(
            Path path, long startFilePos, long endFilePos, short year, ArchDBRTypes type) throws IOException {
        DBR2PBTypeMapping mapping = DBR2PBTypeMapping.getPBClassFor(type);
        unmarshallingConstructor = mapping.getUnmarshallingFromByteArrayConstructor();
        assert (startFilePos >= 0);
        assert (endFilePos >= 0);
        assert (endFilePos >= startFilePos);
        this.year = year;
        lbs = new LineByteStream(path, startFilePos, endFilePos);
        lbs.seekToFirstNewLine();
    }

    @Override
    public boolean hasNext() {
        try {
            if (nextLine.isEmpty() || lineUsed) {
                lbs.readLine(nextLine);
                lineUsed = false;
            }
            if (!nextLine.isEmpty()) return true;
        } catch (Exception ex) {
            logger.error("Exception creating event object", ex);
        }
        return false;
    }

    @Override
    public Event next() {
        try {
            if (nextLine.isEmpty() || lineUsed) {
                lbs.readLine(nextLine);
            }
            Event e = unmarshallingConstructor.newInstance(year, nextLine);
            lineUsed = true;
            return e;
        } catch (Exception ex) {
            logger.error("Exception creating event object", ex);
            return null;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void close() {
        lbs.safeClose();
    }
}
