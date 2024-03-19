package org.epics.archiverappliance.etl;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.common.BasicContext;

/**
 * Event streams can optionally implement bulk transfer friendly methods.
 * If this interface is implemented, then ETL code will use bulk transfers whan moving data.
 * @author mshankar
 *
 */
public interface ETLBulkStream extends EventStream {
	
	/**
	 * Get the first event in this event stream.
	 * If there are no events in this stream, return null.
	 * @param context BasicContext 
	 * @return Event return the first event, or null
	 * @throws IOException  &emsp;
	 */
	public Event getFirstEvent(BasicContext context) throws IOException;
	
	
	/**
	 * Get a byte channel positioned at the first event (after the header).
	 * @param context  BasicContext  
	 * @return ReadableByteChannel A channel that can read bytes. 
	 * @throws IOException &emsp;
	 * @see  <a href="https://docs.oracle.com/javase/8/docs/api/java/nio/channels/ReadableByteChannel.html">java.nio.channels.ReadableByteChannel</a>
	 */
	public ReadableByteChannel getByteChannel(BasicContext context) throws IOException;

}
