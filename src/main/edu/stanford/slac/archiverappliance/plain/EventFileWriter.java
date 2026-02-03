package edu.stanford.slac.archiverappliance.plain;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;

import java.io.IOException;

/**
 * Interface that defines the contract for writing events to a file.
 */
public interface EventFileWriter extends AutoCloseable {
    /**
     * Append a single event to the currently open file.
     * @param event The event to append
     * @throws IOException
     */
    void append(Event event) throws IOException;

    @Override
    void close() throws IOException;

    /**
     * Write the entire stream to the file.
     * @param stream The stream of events to write
     * @throws IOException
     */
    default void writeStreamToFile(EventStream stream) throws IOException {
        for (Event event : stream) {
            append(event);
        }
    }
}
