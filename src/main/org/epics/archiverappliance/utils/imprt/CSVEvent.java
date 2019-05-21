/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.utils.imprt;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.data.ScalarValue;
import org.epics.archiverappliance.data.VectorStringSampleValue;
import org.epics.archiverappliance.data.VectorValue;


/**
 * 
 * @author mshankar
 *
 */
public class CSVEvent implements DBRTimeEvent {
	Timestamp timestamp;
	int status;
	int severity;
	SampleValue sampleValue;
	private ArchDBRTypes type;
	
	public CSVEvent(String[] line, ArchDBRTypes type) throws Exception {
		this.type = type;
		// This line is of this format epochseconds, nanos, value, status, severity
		// Example: 1301986801,446452000,5.55269,0,0
		// Waveforms are pipe escaped....
		if(line == null || line.length < 5) throw new Exception("We need at least five columns in the CSV - epochseconds, nanos, value, status, severity. Example: - 1301986801,446452000,5.55269,0,0");
		// Per Bob, the epochseconds here is EPICS epoch seconds. We need to convert to Java epoch seconds; so we add the offset.
		long epochSeconds = Long.parseLong(line[0]) + TimeUtils.EPICS_EPOCH_2_JAVA_EPOCH_OFFSET;
		int nanos = Integer.parseInt(line[1]);
		timestamp = TimeUtils.convertFromEpochSeconds(epochSeconds, nanos);
		String valueStr = line[2];
		String[] vectorValueStr = valueStr.split("\\|");
		status = Integer.parseInt(line[3]);
		severity = Integer.parseInt(line[4]);
		switch(type) {
		case DBR_SCALAR_STRING:
			sampleValue = new ScalarStringSampleValue(valueStr);
			break;
		case DBR_SCALAR_SHORT:
			sampleValue = new ScalarValue<Short>(Short.valueOf(valueStr));
			break;
		case DBR_SCALAR_FLOAT:
			sampleValue = new ScalarValue<Float>(Float.valueOf(valueStr));
			break;       
		case DBR_SCALAR_ENUM:
			sampleValue = new ScalarValue<Short>(Short.valueOf(valueStr));
			break;
		case DBR_SCALAR_BYTE:
			sampleValue = new ScalarValue<Byte>(Byte.valueOf(valueStr));
			break;
		case DBR_SCALAR_INT:
			sampleValue = new ScalarValue<Integer>(Integer.valueOf(valueStr));
			break;
		case DBR_SCALAR_DOUBLE:
			sampleValue = new ScalarValue<Double>(Double.valueOf(valueStr));
			break;
		case DBR_WAVEFORM_STRING:
			if(valueStr.equals("")) {
				sampleValue = new VectorStringSampleValue(Arrays.asList(new String[0]));
			} else {
				sampleValue = new VectorStringSampleValue(Arrays.asList(vectorValueStr));
			}
			break;      
		case DBR_WAVEFORM_SHORT:
		{
			if(valueStr.equals("")) {
				sampleValue = new VectorValue<Short>(Arrays.asList(new Short[0]));				
			} else {
				ArrayList<Short> vals = new ArrayList<Short>(vectorValueStr.length);
				for(String val : vectorValueStr) {
					vals.add(Short.valueOf(val));
				}
				sampleValue = new VectorValue<Short>(vals);				
			}
		}
		break;         
		case DBR_WAVEFORM_FLOAT:
		{
			if(valueStr.equals("")) {
				sampleValue = new VectorValue<Float>(Arrays.asList(new Float[0]));				
			} else {
				ArrayList<Float> vals = new ArrayList<Float>(vectorValueStr.length);
				for(String val : vectorValueStr) {
					vals.add(Float.valueOf(val));
				}
				sampleValue = new VectorValue<Float>(vals);
			}
		}
		break;       
		case DBR_WAVEFORM_ENUM:
		{
			if(valueStr.equals("")) {
				sampleValue = new VectorValue<Short>(Arrays.asList(new Short[0]));				
			} else {
				ArrayList<Short> vals = new ArrayList<Short>(vectorValueStr.length);
				for(String val : vectorValueStr) {
					vals.add(Short.valueOf(val));
				}
				sampleValue = new VectorValue<Short>(vals);
			}
		}
		break;
		case DBR_WAVEFORM_BYTE:
		{
			if(valueStr.equals("")) {
				sampleValue = new VectorValue<Byte>(Arrays.asList(new Byte[0]));				
			} else {
				ArrayList<Byte> vals = new ArrayList<Byte>(vectorValueStr.length);
				for(String val : vectorValueStr) {
					vals.add(Byte.valueOf(val));
				}
				sampleValue = new VectorValue<Byte>(vals);
			}
		}
		break;
		case DBR_WAVEFORM_INT:
		{
			if(valueStr.equals("")) {
				sampleValue = new VectorValue<Integer>(Arrays.asList(new Integer[0]));				
			} else {
				ArrayList<Integer> vals = new ArrayList<Integer>(vectorValueStr.length);
				for(String val : vectorValueStr) {
					vals.add(Integer.valueOf(val));
				}
				sampleValue = new VectorValue<Integer>(vals);
			}
		}
		break;
		case DBR_WAVEFORM_DOUBLE:
		{
			if(valueStr.equals("")) {
				sampleValue = new VectorValue<Double>(Arrays.asList(new Double[0]));				
			} else {
				ArrayList<Double> vals = new ArrayList<Double>(vectorValueStr.length);
				for(String val : vectorValueStr) {
					vals.add(Double.valueOf(val));
				}
				sampleValue = new VectorValue<Double>(vals);
			}
		}
		break;
		case DBR_V4_GENERIC_BYTES:
			sampleValue = new ScalarStringSampleValue(valueStr);
			break;
			
		default:
			throw new Exception("Unsupported DBR type in swicth statement " + type.toString());
		}
		assert(sampleValue != null);
	}	

	public CSVEvent(CSVEvent src) {
		super();
		this.timestamp = src.timestamp;
		this.status = src.status;
		this.severity = src.severity;
		this.sampleValue = src.sampleValue;
	}


	@Override
	public Event makeClone() {
		return new CSVEvent(this);
	}

	@Override
	public int getStatus() {
		return status;
	}

	@Override
	public int getSeverity() {
		return severity;
	}

	@Override
	public int getRepeatCount() {
		return 0;
	}
	
	@Override
	public void setRepeatCount(int repeatCount) {
		return;
	}


	@Override
	public Timestamp getEventTimeStamp() {
		return timestamp;
	}

	@Override
	public long getEpochSeconds() {
		return TimeUtils.convertToEpochSeconds(timestamp);
	}

	@Override
	public ByteArray getRawForm() {
		throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
	}

	@Override
	public SampleValue getSampleValue() {
		return sampleValue;
	}

	@Override
	public boolean hasFieldValues() {
		return false;
	}

	@Override
	public boolean isActualChange() {
		return false;
	}

	@Override
	public HashMap<String, String> getFields() {
		throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
	}

	@Override
	public String getFieldValue(String fieldName) {
		throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
	}

	@Override
	public void addFieldValue(String fieldName, String fieldValue) {
		throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
	}

	@Override
	public void markAsActualChange() {
		throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
	}

	@Override
	public void setFieldValues(HashMap<String, String> fieldValues, boolean markAsActualChange) {
		throw new UnsupportedOperationException("Not supported. Convert to a PB form if you want to use this.");
	}
	
	
	public static String toString(SampleValue val, ArchDBRTypes type) throws IOException {
		if(val == null) return "";
		
		switch(type) {
		case DBR_SCALAR_STRING:
		case DBR_SCALAR_SHORT:
		case DBR_SCALAR_FLOAT:
		case DBR_SCALAR_ENUM:
		case DBR_SCALAR_BYTE:
		case DBR_SCALAR_INT:
		case DBR_SCALAR_DOUBLE:
		case DBR_V4_GENERIC_BYTES:
			return val.toString();
		case DBR_WAVEFORM_STRING:
		case DBR_WAVEFORM_ENUM:{
			int elementCount = val.getElementCount();
			boolean first = true;
			StringWriter buf = new StringWriter();
			for(int i = 0; i < elementCount; i++) { 
				String elemVal = val.getStringValue(i);
				if(first) { first = false; } else {buf.append("|"); }
				buf.append(elemVal);
			}
			return buf.toString();
		}
		case DBR_WAVEFORM_SHORT:
		case DBR_WAVEFORM_FLOAT:
		case DBR_WAVEFORM_BYTE:
		case DBR_WAVEFORM_INT:
		case DBR_WAVEFORM_DOUBLE: {
			int elementCount = val.getElementCount();
			boolean first = true;
			StringWriter buf = new StringWriter();
			for(int i = 0; i < elementCount; i++) { 
				String elemVal = val.getValue(i).toString();
				if(first) { first = false; } else {buf.append("|"); }
				buf.append(elemVal);
			}
			return buf.toString();
		}
		default:
			throw new IOException("Unsupported DBR type in switch statement " + type.toString());
		}
	}

	@Override
	public ArchDBRTypes getDBRType() {
		return this.type;
	}
	
	@Override
	public void setStatus(int status) {
		this.status = status;
	}
	
	@Override
	public void setSeverity(int severity) {
		this.severity = severity;
	}

}


