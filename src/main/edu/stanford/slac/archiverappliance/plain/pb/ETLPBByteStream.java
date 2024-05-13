package edu.stanford.slac.archiverappliance.plain.pb;

import org.epics.archiverappliance.common.BasicContext;
import org.epics.archiverappliance.etl.ETLBulkStream;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * Event streams can optionally implement bulk transfer friendly methods.
 * If this interface is implemented, then ETL code will use bulk transfers whan moving data.
 *
 * @author mshankar
 */
public interface ETLPBByteStream extends ETLBulkStream {

    /**
     * Get a byte channel positioned at the first event (after the header).
     *
     * @param context BasicContext
     * @return ReadableByteChannel A channel that can read bytes.
     * @throws IOException &emsp;
     * @see <a href="https://docs.oracle.com/javase/8/docs/api/java/nio/channels/ReadableByteChannel.html">java.nio.channels.ReadableByteChannel</a>
     */
    public ReadableByteChannel getByteChannel(BasicContext context) throws IOException;
}
