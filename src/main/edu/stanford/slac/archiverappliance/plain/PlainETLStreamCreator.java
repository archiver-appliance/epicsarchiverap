/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.etl.ETLStreamCreator;

import java.io.IOException;
import java.nio.file.Path;

/**
 * A stream creator that is backed by a single file.
 *
 * @author mshankar
 */
public class PlainETLStreamCreator implements ETLStreamCreator {
    private final String pvName;
    private final Path path;
    private final FileInfo info;

    private final PlainStreams plainStreams;

    public PlainETLStreamCreator(String pvName, Path path, FileInfo fileinfo, PlainStreams plainStreams) {
        this.pvName = pvName;
        this.path = path;
        this.info = fileinfo;
        this.plainStreams = plainStreams;
    }

    @Override
    public EventStream getStream() throws IOException {
        return plainStreams.getStream(pvName, path, info.getType());
    }
}
