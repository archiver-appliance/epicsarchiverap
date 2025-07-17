package edu.stanford.slac.archiverappliance.PB.data;

import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.TypeSystem;
import org.epics.archiverappliance.data.DBRTimeEvent;

import java.lang.reflect.Constructor;

/**
 * TypeSystem for SLAC PB types.
 * @author mshankar
 *
 */
public class PBTypeSystem implements TypeSystem {

    @Override
    public Constructor<? extends DBRTimeEvent> getJCADBRConstructor(ArchDBRTypes archDBRType) {
        return EPICS2PBTypeMapping.getPBClassFor(archDBRType).getJCADBRConstructor();
    }

    @Override
    public Constructor<? extends DBRTimeEvent> getUnmarshallingFromByteArrayConstructor(ArchDBRTypes archDBRType) {
        return DBR2PBTypeMapping.getPBClassFor(archDBRType).getUnmarshallingFromByteArrayConstructor();
    }

    @Override
    public Constructor<? extends DBRTimeEvent> getSerializingConstructor(ArchDBRTypes archDBRType) {
        return DBR2PBTypeMapping.getPBClassFor(archDBRType).getSerializingConstructor();
    }

    @Override
    public Constructor<? extends DBRTimeEvent> getV4Constructor(ArchDBRTypes archDBRType) {
        return EPICS2PBTypeMapping.getPBClassFor(archDBRType).getEPICSV4DBRConstructor();
    }
}
