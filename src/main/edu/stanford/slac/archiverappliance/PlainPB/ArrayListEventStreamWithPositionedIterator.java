package edu.stanford.slac.archiverappliance.PlainPB;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BiDirectionalIterable;
import org.epics.archiverappliance.common.BiDirectionalIterable.IterationDirection;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

/*
 * If we have a small enough file, we may just as well load the entire file into memory and use that as an EventStream
 * This gets around various boundary conditions and is probably more efficient.
 */
public class ArrayListEventStreamWithPositionedIterator implements EventStream, RemotableOverRaw  {
    private static final Logger logger = LogManager.getLogger(ArrayListEventStreamWithPositionedIterator.class.getName());
    private final ArrayListEventStream thestrm;

    public ArrayListEventStreamWithPositionedIterator(
            String pvName,
            Path path,
            Instant startAtTime,
            ArchDBRTypes archDBRTypes,
            BiDirectionalIterable.IterationDirection direction)
            throws IOException {
        logger.debug("Loading {} into memory and starting at {}", path.toAbsolutePath().toString(), TimeUtils.convertToHumanReadableString(startAtTime));
        List<Event> events = new LinkedList<Event>();
        try(EventStream is = new FileBackedPBEventStream(pvName, path, archDBRTypes)) {
            thestrm = new ArrayListEventStream(0, (RemotableEventStreamDesc) is.getDescription());        
            for(Event ev : is) {
                Instant ts = ev.getEventTimeStamp();
                if(direction == IterationDirection.BACKWARDS) {
                    if(ts.equals(startAtTime) || ts.isBefore(startAtTime)) {
                        events.add(ev);
                    }
                } else {
                    if(ts.equals(startAtTime) || ts.isAfter(startAtTime)) {
                        events.add(ev);
                    }
                }
            }
        }
        if(direction == IterationDirection.BACKWARDS) {
            events = events.reversed();
        }
        for(Event ev : events) {
            logger.debug("Adding ev at {}", TimeUtils.convertToHumanReadableString(ev.getEventTimeStamp()));
            thestrm.add(ev);
        }
    }

    @Override
    public Iterator<Event> iterator() {
        return thestrm.iterator();
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public RemotableEventStreamDesc getDescription() {
        return thestrm.getDescription();
    }
    
}
