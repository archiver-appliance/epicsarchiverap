/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.imprt;

import edu.stanford.slac.archiverappliance.PlainPB.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.PlainPB.PBFileInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

/**
 * This is the reverse of ImportCSV. This generates a CSV file to stdout
 * CSV file format is the one used by Bob Hall for export from ChannelArchiver - EPICS epochseconds, nanos, value, status, severity.
 * Example: - 644223600,461147000,5.59054,0,0
 * @author mshankar
 *
 */
public class ExportCSV {

    private static final Logger logger = LogManager.getLogger(ExportCSV.class);

    /**
     * Pass in a PB file.
     *
     * @param args &emsp;
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java org.epics.archiverappliance.utils.imprt.ExportCSV <PBFileName>");
            return;
        }

        String fileName = args[0];
        FileBackedPBEventStream strm = null;
        long lineNumber = 0;
        try {
            Path path = Paths.get(fileName);
            PBFileInfo info = new PBFileInfo(path);
            strm = new FileBackedPBEventStream(info.getPVName(), path, info.getType());
            for (Event e : strm) {
                DBRTimeEvent evnt = (DBRTimeEvent) e;
                Instant ts = evnt.getEventTimeStamp();
                long epicsEpochSeconds = e.getEpochSeconds() - TimeUtils.EPICS_EPOCH_2_JAVA_EPOCH_OFFSET;

                System.out.println(epicsEpochSeconds + "," + ts.getNano()
                        + "," + evnt.getSampleValue().toString()
                        + "," + evnt.getStatus()
                        + "," + evnt.getSeverity());
                lineNumber++;
            }
        } catch (Exception ex) {
            logger.error("Exception near line " + lineNumber, ex);
        } finally {
            try {
                if (strm != null) {
                    strm.close();
                    strm = null;
                }
            } catch (Exception ex) {
            }
        }
    }
}
