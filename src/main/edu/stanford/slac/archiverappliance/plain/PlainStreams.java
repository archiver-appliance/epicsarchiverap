package edu.stanford.slac.archiverappliance.plain;

import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BiDirectionalIterable;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;

public interface PlainStreams {

    EventStream getTimeStream(
            String pvName, Path path, ArchDBRTypes dbrType, Instant start, Instant end, boolean skipSearch)
            throws IOException;

    EventStream getTimeStream(
            String pvName, Path path, Instant start, Instant end, boolean skipSearch, FileInfo fileInfo)
            throws IOException;

    EventStream getStream(String pvName, Path path, ArchDBRTypes dbrType) throws IOException;

    EventStream getStreamForIteration(String pvName, Path path, Instant startAtTime, ArchDBRTypes type, BiDirectionalIterable.IterationDirection direction) throws IOException;
}
