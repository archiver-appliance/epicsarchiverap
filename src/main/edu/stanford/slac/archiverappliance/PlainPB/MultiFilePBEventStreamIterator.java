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
import java.util.Iterator;

/**
 * The iterator for MultiFilePBEventStream's
 * All we do is go thru the streams in sequence and then return the events one after the other.
 * @author mshankar
 *
 */
public class MultiFilePBEventStreamIterator implements Iterator<Event> {
    private static Logger logger = LogManager.getLogger(MultiFilePBEventStreamIterator.class.getName());
    private short year = 0;
    private ArchDBRTypes type;
    private DBR2PBTypeMapping mapping;
    private Constructor<? extends DBRTimeEvent> unmarshallingConstructor;
    private LineByteStreamCreator istreams[];
    private int currentStreamIndex = 0;
    private LineByteStream currentLis;
    private byte[] nextLine = null;

    public MultiFilePBEventStreamIterator(
            LineByteStreamCreator istreams[], String pvName, short year, ArchDBRTypes type) throws IOException {
        this.istreams = istreams;
        this.type = type;
        this.year = year;
        mapping = DBR2PBTypeMapping.getPBClassFor(this.type);
        unmarshallingConstructor = mapping.getUnmarshallingFromByteArrayConstructor();
        currentLis = istreams[currentStreamIndex].getLineByteStream();
    }

    @Override
    public boolean hasNext() {
        try {
            nextLine = currentLis.readLine();
            if (nextLine != null) return true;
            while (true) {
                currentStreamIndex++;
                if (currentStreamIndex >= istreams.length) {
                    logger.debug("All lis's are finished");
                    return false;
                } else {
                    if (currentLis != null) currentLis.safeClose();
                    currentLis = istreams[currentStreamIndex].getLineByteStream();
                    logger.debug("Switching to next lis " + currentLis.getAbsolutePath());
                }
                nextLine = currentLis.readLine();
                if (nextLine != null) return true;
            }
        } catch (Exception ex) {
            logger.error("Exception creating event object", ex);
        }
        return false;
    }

    @Override
    public Event next() {
        try {
            return (Event) unmarshallingConstructor.newInstance(year, new ByteArray(nextLine));
        } catch (Exception ex) {
            logger.error("Exception creating event object", ex);
            return null;
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void close() {}
}
