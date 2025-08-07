package edu.stanford.slac.archiverappliance.plain;

import edu.stanford.slac.archiverappliance.plain.pb.PBFileInfo;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BiDirectionalIterable;
import org.epics.archiverappliance.config.ArchDBRTypes;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

public interface PlainStreams {

    EventStream getTimeStream(
            String pvName, Path path, ArchDBRTypes dbrType, Instant start, Instant end, boolean skipSearch)
            throws IOException;

    EventStream getTimeStream(
            String pvName, Path path, Instant start, Instant end, boolean skipSearch, PBFileInfo fileInfo)
            throws IOException;

    EventStream getStream(String pvName, Path path, ArchDBRTypes dbrType) throws IOException;

    Event dataAtTime(
            List<Path> pathList,
            String pvName,
            Instant atTime,
            Instant startAtTime,
            BiDirectionalIterable.IterationDirection iterationDirection)
            throws IOException;
}
