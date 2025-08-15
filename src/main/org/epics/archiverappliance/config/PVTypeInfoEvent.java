package org.epics.archiverappliance.config;

/**
 * Called when add/remove/update's are made on PVTypeInfo's
 *
 * @author mshankar
 */
public record PVTypeInfoEvent(
        String pvName, PVTypeInfo typeInfo, org.epics.archiverappliance.config.PVTypeInfoEvent.ChangeType changeType) {
    public enum ChangeType {
        TYPEINFO_ADDED,
        TYPEINFO_MODIFIED,
        TYPEINFO_DELETED
    }
}
