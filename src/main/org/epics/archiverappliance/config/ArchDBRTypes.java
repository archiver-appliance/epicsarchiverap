/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import java.util.HashMap;
import java.util.LinkedList;

import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent.PayloadType;


/**
 * The various DBR types supported by the EPICS appliance archiver.
 * We can either follow the names as they are defined in db_access.h (which makes int's a 16 bit java short) or we can follow JCA's names which makes this closer to Java.
 * For now, we follow JCA to minimize confusion for Java developers.
 * @author mshankar
 *
 */
/**
 * @author mshankar
 *
 */
public enum ArchDBRTypes {
	DBR_SCALAR_STRING(0, false, "String", PayloadType.SCALAR_STRING, true),      
	DBR_SCALAR_SHORT(1, false, "short", PayloadType.SCALAR_SHORT, true),         
	DBR_SCALAR_FLOAT(2, false, "float", PayloadType.SCALAR_FLOAT, true),       
	DBR_SCALAR_ENUM(3, false, "enum", PayloadType.SCALAR_ENUM, true),
	DBR_SCALAR_BYTE(4, false, "byte", PayloadType.SCALAR_BYTE, true),
	DBR_SCALAR_INT(5, false, "int", PayloadType.SCALAR_INT, true),
	DBR_SCALAR_DOUBLE(6, false, "double", PayloadType.SCALAR_DOUBLE, true),
	DBR_WAVEFORM_STRING(7, true, "String", PayloadType.WAVEFORM_STRING, true),      
	DBR_WAVEFORM_SHORT(8, true, "short", PayloadType.WAVEFORM_SHORT, true),         
	DBR_WAVEFORM_FLOAT(9, true, "float", PayloadType.WAVEFORM_FLOAT, true),       
	DBR_WAVEFORM_ENUM(10, true, "enum", PayloadType.WAVEFORM_ENUM, true),
	DBR_WAVEFORM_BYTE(11, true, "byte", PayloadType.WAVEFORM_BYTE, true),
	DBR_WAVEFORM_INT(12, true, "int", PayloadType.WAVEFORM_INT, true),
	DBR_WAVEFORM_DOUBLE(13, true, "double", PayloadType.WAVEFORM_DOUBLE, true),
	DBR_V4_GENERIC_BYTES(14, true, "v4generic", PayloadType.V4_GENERIC_BYTES, false);
	
	public static final String V4PREFIX = "EPICSV4://";

	private static HashMap<PayloadType, ArchDBRTypes> PBTypeReverseMapping = new HashMap<PayloadType, ArchDBRTypes>();

	static {
		// Compute reverse lookups;
		for(ArchDBRTypes type : ArchDBRTypes.values()) {
			PBTypeReverseMapping.put(type.getPBPayloadType(), type);
		}
	}

	
	private int integerMap;
	private String primitiveName;
	private boolean isWaveForm = false;
	private PayloadType PBPayloadType;
	private boolean isV3Type = true;
	
	/**
	 * The integerMap is the value from db_access.h.
	 * We get the archiver specific JCA type from the JCA javadoc.
	 * JCA subsumes both scalars and vectors into a single type system; we separate them for space reasons.
	 * @param fromdbaccess_h
	 * @param d
	 * @param isWaveform
	 */
	private ArchDBRTypes(int fromdbaccess_h, boolean isWaveform, String primitivename, PayloadType payloadType, boolean isV3Type) {
		this.integerMap = fromdbaccess_h;
		this.primitiveName = primitivename;
		this.isWaveForm = isWaveform;
		this.PBPayloadType = payloadType;
		this.isV3Type = isV3Type;
	}
	
	public String getPrimitiveName() {
		return primitiveName;
	}

	/**
	 * Get a integer assigned to this type as defined in db_access.h 
	 * @return
	 */
	public int getIntegerMap() {
		return integerMap;
	}

	/**
	 * Is this thing a vector (or in EPICS terms, a waveform).
	 * @return
	 */
	public boolean isWaveForm() {
		return isWaveForm;
	}

	/**
	 * This is used to reverse map from the PB Payloadtype enum into a DBR type
	 * @param intval
	 * @return
	 */
	public static ArchDBRTypes valueOf(PayloadType payloadtype) {
		return PBTypeReverseMapping.get(payloadtype);
	}

	public PayloadType getPBPayloadType() {
		return PBPayloadType;
	}

	public boolean isV3Type() {
		return isV3Type;
	}

	public static SampleValue sampleValueFromString(ArchDBRTypes dbrType, String sampleValueStr) {
		switch(dbrType) {
		case DBR_SCALAR_STRING: { 
			return new ScalarStringSampleValue(sampleValueStr);
			}
		case DBR_SCALAR_SHORT: { 
			return new ScalarValue<Short>(Short.parseShort(sampleValueStr));
			}
		case DBR_SCALAR_FLOAT: { 
			return new ScalarValue<Float>(Float.parseFloat(sampleValueStr));
			}
		case DBR_SCALAR_ENUM: { 
			return new ScalarValue<Short>(Short.parseShort(sampleValueStr));
			}
		case DBR_SCALAR_BYTE: { 
			return new ScalarValue<Byte>(Byte.parseByte(sampleValueStr));
			}
		case DBR_SCALAR_INT: { 
			return new ScalarValue<Integer>(Integer.parseInt(sampleValueStr));
			}
		case DBR_SCALAR_DOUBLE: { 
			return new ScalarValue<Double>(Double.parseDouble(sampleValueStr));
			}
		case DBR_WAVEFORM_STRING: { 
			LinkedList<String> buf = new LinkedList<String>();
			JSONArray vals = (JSONArray) JSONValue.parse(sampleValueStr);
			if(vals != null && !vals.isEmpty()) {
				for(Object val : vals) { 
					buf.add((String) val);
				}
			}
			return new VectorStringSampleValue(buf);
			}
		case DBR_WAVEFORM_SHORT: { 
			LinkedList<Short> buf = new LinkedList<Short>();
			JSONArray vals = (JSONArray) JSONValue.parse(sampleValueStr);
			if(vals != null && !vals.isEmpty()) {
				for(Object val : vals) { 
					buf.add(Short.parseShort((String) val));
				}
			}
			return new VectorValue<Short>(buf);
			}
		case DBR_WAVEFORM_FLOAT: { 
			LinkedList<Float> buf = new LinkedList<Float>();
			JSONArray vals = (JSONArray) JSONValue.parse(sampleValueStr);
			if(vals != null && !vals.isEmpty()) {
				for(Object val : vals) { 
					buf.add(Float.parseFloat((String) val));
				}
			}
			return new VectorValue<Float>(buf);
			}
		case DBR_WAVEFORM_ENUM: { 
			LinkedList<Short> buf = new LinkedList<Short>();
			JSONArray vals = (JSONArray) JSONValue.parse(sampleValueStr);
			if(vals != null && !vals.isEmpty()) {
				for(Object val : vals) { 
					buf.add(Short.parseShort((String) val));
				}
			}
			return new VectorValue<Short>(buf);
			}
		case DBR_WAVEFORM_BYTE: { 
			LinkedList<Byte> buf = new LinkedList<Byte>();
			JSONArray vals = (JSONArray) JSONValue.parse(sampleValueStr);
			if(vals != null && !vals.isEmpty()) {
				for(Object val : vals) { 
					buf.add(Byte.parseByte((String) val));
				}
			}
			return new VectorValue<Byte>(buf);
			}
		case DBR_WAVEFORM_INT: { 
			LinkedList<Integer> buf = new LinkedList<Integer>();
			JSONArray vals = (JSONArray) JSONValue.parse(sampleValueStr);
			if(vals != null && !vals.isEmpty()) {
				for(Object val : vals) { 
					buf.add(Integer.parseInt((String) val));
				}
			}
			return new VectorValue<Integer>(buf);
			}
		case DBR_WAVEFORM_DOUBLE: { 
			LinkedList<Double> buf = new LinkedList<Double>();
			JSONArray vals = (JSONArray) JSONValue.parse(sampleValueStr);
			if(vals != null && !vals.isEmpty()) {
				for(Object val : vals) { 
					buf.add(Double.parseDouble((String) val));
				}
			}
			return new VectorValue<Double>(buf);
			}
		case DBR_V4_GENERIC_BYTES: {
			throw new UnsupportedOperationException("We do not support this for V4 types yet.");
		}
		default:
			throw new UnsupportedOperationException("When we add a new DBR_TYPE, we should add some code here.");
		}
	}

}