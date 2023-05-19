/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;


import java.lang.reflect.Constructor;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;


/**
 * Maps ArchDBRTypes to PB classes. 
 * This functions much like a methodtable.
 * @author mshankar
 *
 */
public class DBR2PBTypeMapping {
	private static Logger logger = LogManager.getLogger(DBR2PBTypeMapping.class.getName());
	private static HashMap<ArchDBRTypes, DBR2PBTypeMapping> typemap = new HashMap<ArchDBRTypes, DBR2PBTypeMapping>();
	static {
		typemap.put(ArchDBRTypes.DBR_SCALAR_STRING, new DBR2PBTypeMapping(PBScalarString.class));      
		typemap.put(ArchDBRTypes.DBR_SCALAR_SHORT, new DBR2PBTypeMapping(PBScalarShort.class));        
		typemap.put(ArchDBRTypes.DBR_SCALAR_FLOAT, new DBR2PBTypeMapping(PBScalarFloat.class));       
		typemap.put(ArchDBRTypes.DBR_SCALAR_ENUM, new DBR2PBTypeMapping(PBScalarEnum.class));
		typemap.put(ArchDBRTypes.DBR_SCALAR_BYTE, new DBR2PBTypeMapping(PBScalarByte.class));
		typemap.put(ArchDBRTypes.DBR_SCALAR_INT, new DBR2PBTypeMapping(PBScalarInt.class));
		typemap.put(ArchDBRTypes.DBR_SCALAR_DOUBLE, new DBR2PBTypeMapping(PBScalarDouble.class));
		typemap.put(ArchDBRTypes.DBR_WAVEFORM_STRING, new DBR2PBTypeMapping(PBVectorString.class));      
		typemap.put(ArchDBRTypes.DBR_WAVEFORM_SHORT, new DBR2PBTypeMapping(PBVectorShort.class));         
		typemap.put(ArchDBRTypes.DBR_WAVEFORM_FLOAT, new DBR2PBTypeMapping(PBVectorFloat.class));       
		typemap.put(ArchDBRTypes.DBR_WAVEFORM_ENUM, new DBR2PBTypeMapping(PBVectorEnum.class));
		typemap.put(ArchDBRTypes.DBR_WAVEFORM_BYTE, new DBR2PBTypeMapping(PBVectorByte.class));
		typemap.put(ArchDBRTypes.DBR_WAVEFORM_INT, new DBR2PBTypeMapping(PBVectorInt.class));
		typemap.put(ArchDBRTypes.DBR_WAVEFORM_DOUBLE, new DBR2PBTypeMapping(PBVectorDouble.class));
		typemap.put(ArchDBRTypes.DBR_V4_GENERIC_BYTES, new DBR2PBTypeMapping(PBV4GenericBytes.class));
		
		for(ArchDBRTypes t : ArchDBRTypes.values()) {
			if(typemap.get(t) == null) {
				throw new RuntimeException("We have a type in DBR type that does have an equivalent PB type");
			}
		}
		
	}

	public static DBR2PBTypeMapping getPBClassFor(ArchDBRTypes type) {
		return typemap.get(type);
	}
	

	Class<? extends DBRTimeEvent> pbclass;
	private Constructor<? extends DBRTimeEvent> unmarshallingFromByteArrayConstructor;
	private Constructor<? extends DBRTimeEvent> serializingConstructor;
	
	private DBR2PBTypeMapping(Class<? extends DBRTimeEvent> pblass) {
		this.pbclass = pblass;
		try {
			unmarshallingFromByteArrayConstructor = this.pbclass.getConstructor(Short.TYPE, ByteArray.class);
		} catch (Exception ex) {
			logger.error("Cannot get unmarshalling constructor from ByteArray for PB event for class " + pbclass.getName(), ex);
			throw new RuntimeException("Cannot get unmarshalling constructor from ByteArray for PB event for class " + pbclass.getName());
		}
		
		
		try {
			serializingConstructor = this.pbclass.getConstructor(DBRTimeEvent.class);
		} catch (Exception ex) {
			logger.error("Cannot get serializing constructor for PB event for class " + pbclass.getName(), ex);
			throw new RuntimeException("Cannot get serializing constructor for PB event for class " + pbclass.getName());
		}

		
		assert(unmarshallingFromByteArrayConstructor != null);
		assert(serializingConstructor != null);
	}


	public Constructor<? extends DBRTimeEvent> getSerializingConstructor() {
		return serializingConstructor;
	}

	public Constructor<? extends DBRTimeEvent> getUnmarshallingFromByteArrayConstructor() {
		return unmarshallingFromByteArrayConstructor;
	}

}
