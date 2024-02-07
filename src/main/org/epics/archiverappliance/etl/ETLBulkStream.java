package org.epics.archiverappliance.etl;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;

import java.io.IOException;

public interface ETLBulkStream extends EventStream {

    /**
     * Get the first event in this event stream.
     * If there are no events in this stream, return null.
     *
     * @param context BasicContext
     * @return Event return the first event, or null
     * @throws IOException &emsp;
     */
    Event getFirstEvent(BasicContext context) throws IOException;
}
