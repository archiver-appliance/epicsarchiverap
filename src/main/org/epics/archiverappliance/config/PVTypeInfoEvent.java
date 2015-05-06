package org.epics.archiverappliance.config;

/**
 * Called when add/remove/update's are made on PVTypeInfo's
 * @author mshankar
 *
 */
public class PVTypeInfoEvent {
	public enum ChangeType { TYPEINFO_ADDED, TYPEINFO_MODIFIED, TYPEINFO_DELETED};
	private String pvName;
	private PVTypeInfo typeInfo;
	private ChangeType changeType;
	
	public PVTypeInfoEvent(String pvName, PVTypeInfo typeInfo, ChangeType changeType) {
		this.pvName = pvName;
		this.typeInfo = typeInfo;
		this.changeType = changeType;
	}

	public String getPvName() {
		return pvName;
	}

	public PVTypeInfo getTypeInfo() {
		return typeInfo;
	}

	public ChangeType getChangeType() {
		return changeType;
	}
}
