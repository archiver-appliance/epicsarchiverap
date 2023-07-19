/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PlainPB;

import java.io.IOException;
import java.nio.file.Path;

import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.etl.ETLStreamCreator;

/**
 * A stream creator that is backed by a single file.
 * @author mshankar
 *
 */
public class FileStreamCreator implements ETLStreamCreator {
    private String pvName;
    private Path path;
    private PBFileInfo info;

    public FileStreamCreator(String pvName, Path path, PBFileInfo fileinfo) {
        this.pvName = pvName;
        this.path = path;
        this.info = fileinfo;
    }

    @Override
    public EventStream getStream() throws IOException {
        return new FileBackedPBEventStream(pvName, path, info.getType());
    }

}
