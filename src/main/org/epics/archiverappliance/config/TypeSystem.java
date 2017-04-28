package org.epics.archiverappliance.config;

import java.lang.reflect.Constructor;

import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.retrieval.channelarchiver.HashMapEvent;

/**
 * Interface for translating from JCA to Event (actually DBRTimeEvents).
 * For SLAC, these are the various PB types in the edu.slac packages...
 * This is mostly configurable at the configservice.  
 *  
 * @author mshankar
 *
 */
public interface TypeSystem {
	
	/**
	 * Use this to create a new Event (actually DBRTimeEvent) from a JCA DBR class.
	 * For example, getJCADBRConstructor().newInstance(dbr) should return you an appropriate Event
	 * @param archDBRType the enumeration type
	 * @return a new DBRTimeEvent
	 * @see org.epics.archiverappliance.config.ArchDBRTypes
	 */
	public Constructor<? extends DBRTimeEvent> getJCADBRConstructor(ArchDBRTypes archDBRType);

	/**
	 * Use this when reading serialized data from EventStreams; for example, FileEventStreams
	 * The constructor takes a short for the year that the data is applicable to and a byte array and gives you a DBRTimeEvent.
	 * For example, getUnmarshallingConstructor().newInstance(yts.getYear(), rawFormAsByteArray)
	 * @param archDBRType the enumeration type	 
	 * @return a new DBRTimeEvent
	 * @see org.epics.archiverappliance.config.ArchDBRTypes
	 */
	public Constructor<? extends DBRTimeEvent> getUnmarshallingFromByteArrayConstructor(ArchDBRTypes archDBRType);

	/**
	 * Use this to convert a DBRTimeEvent that does not support a rawform into one that supports raw form
	 * Used for integration with external datasources where we contruct a nonJCA class that implements DBRTimeEvent and then want to send it across the wire in raw form.
	 * For example, HashMapEvent is a name-value pair that implements most of DBRTimeEvent expect the serializing form.
	 * So, serializingConstructor.newInstance(new HashMapEvent(dbrType, workingCopyOfEvent)) gives you a DBRTimeEvent that supports byte[] getRawForm() 
	 * @param archDBRType the enumeration type
	 * @return a new DBRTimeEvent
	 * @see HashMapEvent
	 * @see org.epics.archiverappliance.config.ArchDBRTypes
	 */
	public Constructor<? extends DBRTimeEvent> getSerializingConstructor(ArchDBRTypes archDBRType);
	
	
	/**
	 * Use this to create a new Event (actually DBRTimeEvent) from a EPICS v4 class.
	 * For example, getV4Constructor().newInstance(Data_EPICSV4 v4Data) should return you an appropriate Event
	 * @param archDBRType the enumeration type
	 * @return a new DBRTimeEvent
	 * @see org.epics.archiverappliance.config.ArchDBRTypes
	 */
	public Constructor<? extends DBRTimeEvent> getV4Constructor(ArchDBRTypes archDBRType);

}
