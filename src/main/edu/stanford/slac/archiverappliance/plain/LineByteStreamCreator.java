/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.PB.utils.LineByteStream;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;
import java.nio.file.Path;

/**
 * When using a MultiFileEventStream on a HOUR granularity, we could potentially run into "too many open files" issues.
 * So we add a level of indirection for this case.
 * @author mshankar
 *
 */
public class LineByteStreamCreator {
    LineByteStream lis;
    Path path;
    String pvName;
    ArchDBRTypes type;

    /**
     * In this case we get a correctly positioned LIS
     * @param lis The line bytes stream
     * @param pvName the PV name
     * @param type  Enum ArchDBRTypes
     */
    public LineByteStreamCreator(LineByteStream lis, String pvName, ArchDBRTypes type) {
        this.lis = lis;
        this.pvName = pvName;
        this.type = type;
    }

    /**
     * In this case, we get a whole file
     * We need to position the lis past the header before returning.
     * @param path Path
     * @param pvName The PV name
     * @param type  Enum ArchDBRTypes
     */
    public LineByteStreamCreator(Path path, String pvName, ArchDBRTypes type) {
        this.path = path;
        this.pvName = pvName;
        this.type = type;
    }

    public LineByteStream getLineByteStream() throws IOException {
        if (lis != null) return lis;

        lis = new LineByteStream(path);
        // Position the lis after the header.
        PBFileInfo.checkPayloadInfo(lis, pvName, type);
        return lis;
    }

    public void safeClose() {
        if (lis != null) {
            lis.safeClose();
        }
    }
}
