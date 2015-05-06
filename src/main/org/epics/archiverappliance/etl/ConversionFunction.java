package org.epics.archiverappliance.etl;

import java.io.IOException;

import org.epics.archiverappliance.EventStream;

/**
 * A conversion function takes in a EventStream and gives you another EventStream that is based on this EventStream.
 * This is used to convert between types and so on. 
 * For example, if a PV used to be DBR_DOUBLE and is now a DBR_ENUM, we can create a custom conversion function that knows how to convert from DBR_DOUBLEs to DBR_ENUMs.
 * So, for example, the BPL for type conversion would be
 * 1) Pause the PV.
 * 2) Consolidate the data all the way to the LTS.
 * 3) Apply the selected conversion function; the generated/converted EventStream is then stored in place of the original stream by the plugin.
 * 4) Change the typeInfo in the database
 * 5) Resume the PV.    
 * 
 * If we encounter an error in the conversion process, throw a ConversionException when generating the converted EventStream.
 * This will prompt the plugin to fail the conversion and clean up.
 *  
 * @author mshankar
 *
 */
public interface ConversionFunction {
	public EventStream convertStream(EventStream srcEventStream) throws IOException;
}