package org.epics.archiverappliance.config;

import org.epics.archiverappliance.config.exception.ConfigException;

/**
 * Interface for converting a PV name to something that forms the prefix of a chunk's key.
 * See the {@link ConvertPVNameToKey default} implementation for more details 
 * @author mshankar
 *
 */
public interface PVNameToKeyMapping {
	public void initialize(ConfigService configService) throws ConfigException;
	/**
	 * Return true if the given pvName contains any site specific separators.
	 * @return
	 */
	public boolean containsSiteSeparators(String pvName);
	public abstract String convertPVNameToKey(String pvName);

}