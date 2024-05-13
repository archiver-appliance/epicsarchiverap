/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.plain.pb.FileBackedPBEventStream;
import edu.stanford.slac.archiverappliance.plain.pb.PBFileInfo;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BiDirectionalIterable;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.etl.ETLStreamCreator;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;

/**
 * A stream creator that is backed by a single file.
 * @author mshankar
 *
 */
public class FileStreamCreator implements ETLStreamCreator {
    private final String pvName;
    private final Path path;
    private final PBFileInfo info;

    public FileStreamCreator(String pvName, Path path, PBFileInfo fileinfo) {
        this.pvName = pvName;
        this.path = path;
        this.info = fileinfo;
    }

    public static EventStream getTimeStream(
            String pvName, Path path, ArchDBRTypes dbrType, Instant start, Instant end, boolean skipSearch)
            throws IOException {

        return new FileBackedPBEventStream(pvName, path, dbrType, start, end, skipSearch);
    }

    public static EventStream getTimeStream(
            String pvName, Path path, Instant start, Instant end, boolean skipSearch, ArchDBRTypes archDBRTypes)
            throws IOException {

        return new FileBackedPBEventStream(pvName, path, archDBRTypes, start, end, skipSearch);
    }

    public static EventStream getStream(String pvName, Path path, ArchDBRTypes dbrType) throws IOException {

        return new FileBackedPBEventStream(pvName, path, dbrType);
    }


    public static EventStream getStreamForIteration(
            String pvName,
            Path path,
            Instant startAtTime,
            ArchDBRTypes archDBRTypes,
            BiDirectionalIterable.IterationDirection direction) throws IOException {

        return new FileBackedPBEventStream(pvName, path, archDBRTypes, startAtTime, direction);
    }


    @Override
    public EventStream getStream() throws IOException {
        return new FileBackedPBEventStream(pvName, path, info.getType());
    }
}
