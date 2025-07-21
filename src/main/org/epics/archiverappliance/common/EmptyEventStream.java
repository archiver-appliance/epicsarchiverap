package org.epics.archiverappliance.common;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.RemotableOverRaw;

import java.io.IOException;
import java.util.Iterator;

/**
 * Instead of returning null, in some cases, it may be more convenient to return a EventStream with no events
 * @author mshankar
 *
 */
public class EmptyEventStream implements EventStream, RemotableOverRaw {
    RemotableEventStreamDesc desc = null;

    public EmptyEventStream(String pvName, ArchDBRTypes type) {
        desc = new RemotableEventStreamDesc(type, pvName, TimeUtils.getCurrentYear());
    }

    @Override
    public Iterator<Event> iterator() {
        return new Iterator<Event>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public Event next() {
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void close() throws IOException {}

    @Override
    public RemotableEventStreamDesc getDescription() {
        return desc;
    }
}
