package org.epics.archiverappliance.config;

import java.util.HashMap;

import org.epics.archiverappliance.engine.pv.EPICSV4.DataType_EPICSV4;


public enum EPICSV42DBRType {
	EPICSV4_MAPPING_GENERIC_BYTES(DataType_EPICSV4.TIME_VSTATIC_BYTES, ArchDBRTypes.DBR_V4_GENERIC_BYTES);

	private DataType_EPICSV4 v4type;
	private ArchDBRTypes archDBRType;

	private EPICSV42DBRType(DataType_EPICSV4 v4Type,  ArchDBRTypes archDBRType) {
		this.v4type = v4Type;
		this.archDBRType = archDBRType;
	}
	
	
	private static HashMap<ArchDBRTypes, DataType_EPICSV4> arch2v4 = new HashMap<ArchDBRTypes, DataType_EPICSV4>();
	static { 
		for(EPICSV42DBRType t : EPICSV42DBRType.values()) {
			arch2v4.put(t.archDBRType, t.v4type);
		}		
	}
	
	
	/**
	 * Get the equivalent archiver data type given a v4
	 * @param d
	 * @return
	 */
	public static ArchDBRTypes valueOf(DataType_EPICSV4 av4Type) {
		for(EPICSV42DBRType t : EPICSV42DBRType.values()) {
			if(t.v4type.equals(av4Type)) {
				return t.archDBRType;
			}
		}
		return null;
	}


}
