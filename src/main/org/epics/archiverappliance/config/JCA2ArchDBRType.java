package org.epics.archiverappliance.config;

import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBRType;

import java.util.HashMap;

import org.apache.log4j.Logger;

public enum JCA2ArchDBRType {
	JCAMAPPING_SCALAR_STRING(DBRType.TIME_STRING, false, ArchDBRTypes.DBR_SCALAR_STRING),      
	JCAMAPPING_SCALAR_SHORT(DBRType.TIME_SHORT, false, ArchDBRTypes.DBR_SCALAR_SHORT),         
	JCAMAPPING_SCALAR_FLOAT(DBRType.TIME_FLOAT, false, ArchDBRTypes.DBR_SCALAR_FLOAT),       
	JCAMAPPING_SCALAR_ENUM(DBRType.TIME_ENUM, false, ArchDBRTypes.DBR_SCALAR_ENUM),
	JCAMAPPING_SCALAR_BYTE(DBRType.TIME_BYTE, false, ArchDBRTypes.DBR_SCALAR_BYTE),
	JCAMAPPING_SCALAR_INT(DBRType.TIME_INT, false, ArchDBRTypes.DBR_SCALAR_INT),
	JCAMAPPING_SCALAR_DOUBLE(DBRType.TIME_DOUBLE, false, ArchDBRTypes.DBR_SCALAR_DOUBLE),
	JCAMAPPING_WAVEFORM_STRING(DBRType.TIME_STRING, true, ArchDBRTypes.DBR_WAVEFORM_STRING),      
	JCAMAPPING_WAVEFORM_SHORT(DBRType.TIME_SHORT, true, ArchDBRTypes.DBR_WAVEFORM_SHORT),         
	JCAMAPPING_WAVEFORM_FLOAT(DBRType.TIME_FLOAT, true, ArchDBRTypes.DBR_WAVEFORM_FLOAT),       
	JCAMAPPING_WAVEFORM_ENUM(DBRType.TIME_ENUM, true, ArchDBRTypes.DBR_WAVEFORM_ENUM),
	JCAMAPPING_WAVEFORM_BYTE(DBRType.TIME_BYTE, true, ArchDBRTypes.DBR_WAVEFORM_BYTE),
	JCAMAPPING_WAVEFORM_INT(DBRType.TIME_INT, true, ArchDBRTypes.DBR_WAVEFORM_INT),
	JCAMAPPING_WAVEFORM_DOUBLE(DBRType.TIME_DOUBLE, true, ArchDBRTypes.DBR_WAVEFORM_DOUBLE);
	
	private static Logger logger = Logger.getLogger(JCA2ArchDBRType.class.getName());
	private DBRType dbrtype;
	private boolean waveform;
	private ArchDBRTypes archDBRType;
	
	private JCA2ArchDBRType(DBRType rawDBRType, boolean isWaveform, ArchDBRTypes archDBRType) {
		this.dbrtype = rawDBRType;
		this.waveform = isWaveform;
		this.archDBRType = archDBRType;
	}
	
	private static HashMap<ArchDBRTypes, DBRType> arch2JCA = new HashMap<ArchDBRTypes, DBRType>();
	static { 
		for(JCA2ArchDBRType t : JCA2ArchDBRType.values()) {
			arch2JCA.put(t.archDBRType, t.dbrtype);
		}		
	}



	/**
	 * Get the equivalent archiver data type given a JCA DBR
	 * @param d JCA DBR
	 * @return ArchDBRTypes  &emsp;
	 */
	public static ArchDBRTypes valueOf(DBR d) {
		boolean isVector = (d.getCount() > 1);
		DBRType dt = d.getType();
		for(JCA2ArchDBRType t : JCA2ArchDBRType.values()) {
			if(t.waveform == isVector && t.dbrtype.equals(dt)) {
				return t.archDBRType;
			}
		}
		logger.error("Cannot determine ArchDBRType for DBRType " + (dt != null ? dt.getName() : "null") + " and count " + d.getCount());
		return null;
	}
	
	private static HashMap<DBRType, DBRType> raw2timeDBRTypemappings = new HashMap<DBRType, DBRType>();
	static {
		raw2timeDBRTypemappings.put(DBRType.STRING, DBRType.TIME_STRING);
		raw2timeDBRTypemappings.put(DBRType.SHORT, DBRType.TIME_SHORT);
		raw2timeDBRTypemappings.put(DBRType.FLOAT, DBRType.TIME_FLOAT);
		raw2timeDBRTypemappings.put(DBRType.ENUM, DBRType.TIME_ENUM);
		raw2timeDBRTypemappings.put(DBRType.BYTE, DBRType.TIME_BYTE);
		raw2timeDBRTypemappings.put(DBRType.INT, DBRType.TIME_INT);
		raw2timeDBRTypemappings.put(DBRType.DOUBLE, DBRType.TIME_DOUBLE);
	}
	
	/**
	 * Get the equivalent archiver data type given a JCA DBRType.
	 * Note that in this case, we are not passing in DBR_TIME_DOUBLE etc; we are passing in DBR_DOUBLE and so on.
	 * So we have an extra step to map from DBR_DOUBLE to DBR_TIME_DOUBLE and then from DBR_TIME_DOUBLE to the appropriate ArchDBRType 
	 * @param dt The JCA DBRType
	 * @param elementCount  &emsp;
	 * @return ArchDBRTypes  &emsp;
	 */
	public static ArchDBRTypes resolveFromCAInfo(DBRType dt, int elementCount) {
		DBRType timedt = raw2timeDBRTypemappings.get(dt);
		boolean isVector = (elementCount > 1);
		for(JCA2ArchDBRType t : JCA2ArchDBRType.values()) {
			if(t.waveform == isVector && t.dbrtype.equals(timedt)) {
				return t.archDBRType;
			}
		}
		return null;
	}
	
	
	/**
	 * Get the JCA type appropriate for this arch dbr type.
	 * @param archDBRTypes  ArchDBRTypes 
	 * @return DBRType The JCA type
	 */
	public static DBRType getJCATypeforArchDBRType(ArchDBRTypes archDBRTypes) {
		return arch2JCA.get(archDBRTypes);
	}
}
