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

    public abstract String convertPVNameToKey(String pvName);
    /**
     * Break a PV name into parts separated by the site specific separators
     * For examples, ABC:123:DEF gets broken into [ABC, 123, DEF]
     * @param pvName The name of PV.
     * @return String Parts separated by separators
     */
    public String[] breakIntoParts(String pvName);
}
