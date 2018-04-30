/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.pva;

import java.io.OutputStream;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.retrieval.RemotableEventStreamDesc;
import org.epics.archiverappliance.retrieval.mimeresponses.MimeResponse;
import org.epics.nt.HasAlarm;
import org.epics.nt.HasTimeStamp;
import org.epics.nt.NTScalar;
import org.epics.nt.NTScalarArray;
import org.epics.pvaccess.server.rpc.RPCResponseCallback;
import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.factory.StatusFactory;
import org.epics.pvdata.property.Alarm;
import org.epics.pvdata.property.AlarmSeverity;
import org.epics.pvdata.property.AlarmStatus;
import org.epics.pvdata.property.PVAlarm;
import org.epics.pvdata.property.PVAlarmFactory;
import org.epics.pvdata.property.PVTimeStamp;
import org.epics.pvdata.property.PVTimeStampFactory;
import org.epics.pvdata.property.TimeStamp;
import org.epics.pvdata.property.TimeStampFactory;
import org.epics.pvdata.pv.Field;
import org.epics.pvdata.pv.FieldCreate;
import org.epics.pvdata.pv.PVByte;
import org.epics.pvdata.pv.PVByteArray;
import org.epics.pvdata.pv.PVDouble;
import org.epics.pvdata.pv.PVDoubleArray;
import org.epics.pvdata.pv.PVFloat;
import org.epics.pvdata.pv.PVFloatArray;
import org.epics.pvdata.pv.PVInt;
import org.epics.pvdata.pv.PVIntArray;
import org.epics.pvdata.pv.PVShort;
import org.epics.pvdata.pv.PVShortArray;
import org.epics.pvdata.pv.PVString;
import org.epics.pvdata.pv.PVStringArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.PVStructureArray;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.Structure;
import org.epics.pvdata.pv.StructureArray;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Floats;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Shorts;

/**
 * @author mshankar The response is a array of PV elements, each PV has a meta
 *         and data section. The data section has timestamp in epoch seconds and
 *         the value
 */
public class PvaMimeResponse implements MimeResponse {
	boolean firstPV = true;
	boolean closePV = false;
	
	private PVTypeInfo typeInfo;
	private PVStructure pvStruct;

	@Override
	public void consumeEvent(Event e) throws Exception {
		
		DBRTimeEvent evnt = (DBRTimeEvent) e;

//		System.out.println("{ \"secs\": " + evnt.getEpochSeconds() 
//		+ ", \"val\": " + evnt.getSampleValue().toJSONString()
//		+ ", \"nanos\": " + Integer.toString(evnt.getEventTimeStamp().getNanos())
//		+ ", \"severity\":" + Integer.toString(evnt.getSeverity())
//		+ ", \"status\":" + Integer.toString(evnt.getStatus())
//		+ " }");
		System.out.println("The stream type is : " + evnt.getDBRType());

		PVStructureArray val = pvStruct.getStructureArrayField("value");
		switch (typeInfo.getDBRType()) {
		case DBR_SCALAR_FLOAT: {
			NTScalar struct = NTScalar.createBuilder().value(ScalarType.pvFloat).addAlarm().addTimeStamp().create();
			struct.getValue(PVFloat.class).put(evnt.getSampleValue().getValue().floatValue());
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_SCALAR_DOUBLE: {
			NTScalar struct = NTScalar.createBuilder().value(ScalarType.pvDouble).addAlarm().addTimeStamp().create();
			struct.getValue(PVDouble.class).put(evnt.getSampleValue().getValue().doubleValue());
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_SCALAR_BYTE: {
			NTScalar struct = NTScalar.createBuilder().value(ScalarType.pvByte).addAlarm().addTimeStamp().create();
			struct.getValue(PVByte.class).put(evnt.getSampleValue().getValue().byteValue());
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_SCALAR_SHORT: {
			NTScalar struct = NTScalar.createBuilder().value(ScalarType.pvShort).addAlarm().addTimeStamp().create();
			struct.getValue(PVShort.class).put(evnt.getSampleValue().getValue().shortValue());
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_SCALAR_INT: {
			NTScalar struct = NTScalar.createBuilder().value(ScalarType.pvInt).addAlarm().addTimeStamp().create();
			struct.getValue(PVInt.class).put(evnt.getSampleValue().getValue().intValue());
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_SCALAR_STRING: {
			NTScalar struct = NTScalar.createBuilder().value(ScalarType.pvString).addAlarm().addTimeStamp().create();
			struct.getValue(PVString.class).put(evnt.getSampleValue().getValue().toString());
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_SCALAR_ENUM: {
			// Not supported
		}
		case DBR_WAVEFORM_FLOAT: {
			NTScalarArray struct = NTScalarArray.createBuilder().value(ScalarType.pvFloat).addAlarm().addTimeStamp().create();
			List<Float> values = evnt.getSampleValue().getValues();
			struct.getValue(PVFloatArray.class).put(0, values.size(), Floats.toArray(values), 0);
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_WAVEFORM_DOUBLE: {
			NTScalarArray struct = NTScalarArray.createBuilder().value(ScalarType.pvDouble).addAlarm().addTimeStamp().create();
			List<Double> values = evnt.getSampleValue().getValues();
			struct.getValue(PVDoubleArray.class).put(0, values.size(), Doubles.toArray(values), 0);
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_WAVEFORM_SHORT: {
			NTScalarArray struct = NTScalarArray.createBuilder().value(ScalarType.pvShort).addAlarm().addTimeStamp().create();
			List<Short> values = evnt.getSampleValue().getValues();
			struct.getValue(PVShortArray.class).put(0, values.size(), Shorts.toArray(values), 0);
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_WAVEFORM_BYTE: {
			NTScalarArray struct = NTScalarArray.createBuilder().value(ScalarType.pvByte).addAlarm().addTimeStamp().create();
			List<Byte> values = evnt.getSampleValue().getValues();
			struct.getValue(PVByteArray.class).put(0, values.size(), Bytes.toArray(values), 0);
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_WAVEFORM_INT: {
			NTScalarArray struct = NTScalarArray.createBuilder().value(ScalarType.pvString).addAlarm().addTimeStamp().create();
			List<Integer> values = evnt.getSampleValue().getValues();
			struct.getValue(PVIntArray.class).put(0, values.size(), Ints.toArray(values), 0);
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_WAVEFORM_STRING: {
			NTScalarArray struct = NTScalarArray.createBuilder().value(ScalarType.pvString).addAlarm().addTimeStamp().create();
			List<String> values = evnt.getSampleValue().getValues();
			struct.getValue(PVStringArray.class).put(0, values.size(), values.toArray(new String[values.size()]), 0);
			addAlarmInfo(struct, evnt);
			addTimeInfo(struct, evnt);
			val.put(val.getLength(), 1, new PVStructure[] { struct.getPVStructure() }, 0);
			break;
		}
		case DBR_WAVEFORM_ENUM:
		case DBR_V4_GENERIC_BYTES: {

		}
		default:
			throw new UnsupportedOperationException("Unknown DBR type " + typeInfo.getDBRType());
		}
	}

	private void addAlarmInfo(HasAlarm struct, DBRTimeEvent evnt) {
		// Put the alarm info
		Alarm alarm = new Alarm();
		alarm.setSeverity(AlarmSeverity.getSeverity(evnt.getSeverity()));
		alarm.setStatus(AlarmStatus.getStatus(evnt.getStatus()));

		PVAlarm pvAlarm = PVAlarmFactory.create();
		pvAlarm.attach(struct.getAlarm());
		pvAlarm.set(alarm);
	}

	private void addTimeInfo(HasTimeStamp struct, DBRTimeEvent evnt) {
		// Put time info
		TimeStamp ts = TimeStampFactory.create();
		ts.put(TimeUtils.convertToEpochSeconds(evnt.getEventTimeStamp()), evnt.getEventTimeStamp().getNanos());

		PVTimeStamp pvTimeStamp = PVTimeStampFactory.create();
		pvTimeStamp.attach(struct.getTimeStamp());
		pvTimeStamp.set(ts);
	}

	@Override
	public void setOutputStream(OutputStream os) {
		// TODO ???
	}

	@Deprecated
	public void setOutput(RPCResponseCallback resp) {
//		this.resp = resp;
	}

	public void setTypeInfo(PVTypeInfo typeInfo) {
		this.typeInfo = typeInfo;
	}

	public void close() {
//		resp.requestDone(StatusFactory.getStatusCreate().getStatusOK(), pvStruct);
		System.out.println("................Closing the PVAMime resource....................");
	}

	@Override
	public void processingPV(String pv, Timestamp start, Timestamp end, EventStreamDesc streamDesc) {
		if (firstPV) {
			firstPV = false;
		}
		RemotableEventStreamDesc remoteDesc = (RemotableEventStreamDesc) streamDesc;
		StringWriter buf = new StringWriter();
		buf.append("{ \"name\": \"").append(pv).append("\" ");
		if (streamDesc != null) {
			HashMap<String, String> headers = remoteDesc.getHeaders();
			if (!headers.isEmpty()) {
				for (String fieldName : headers.keySet()) {
					String fieldValue = headers.get(fieldName);
					if (fieldValue != null && !fieldValue.isEmpty()) {
						buf.append(", \"" + fieldName + "\": \"").append(fieldValue).append("\" ");
					}
				}
			}
		}
		buf.append("}");
		System.out.println(remoteDesc.getPvName());
		this.pvStruct = createResultPVStructure(this.typeInfo);
		NTScalar meta = NTScalar.createBuilder().value(ScalarType.pvString).create();
		meta.getValue(PVString.class).put(buf.toString());
		// TODO this could populate the label
		System.out.println(meta);
		closePV = true;
	}

	public void swicthingToStream(EventStream strm) {
		// Not much to do here for now.
	}

	@Override
	public HashMap<String, String> getExtraHeaders() {
		HashMap<String, String> ret = new HashMap<String, String>();
		// Allow applications served from other URL's to access the JSON data from this
		// server.
		ret.put(MimeResponse.ACCESS_CONTROL_ALLOW_ORIGIN, "*");
		return ret;
	}
	

	/**
	 * Create the PVStructure appropriately setup based on the type info of the pv
	 * @param typeInfo
	 * @return
	 */
	private PVStructure createResultPVStructure(PVTypeInfo typeInfo) {

		FieldCreate fieldCreate = FieldFactory.getFieldCreate();

		StructureArray valueField;
		switch (typeInfo.getDBRType()) {
		case DBR_SCALAR_FLOAT: {
			valueField = fieldCreate.createStructureArray(
					NTScalar.createBuilder().value(ScalarType.pvFloat).addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_SCALAR_DOUBLE: {
			valueField = fieldCreate.createStructureArray(
					NTScalar.createBuilder().value(ScalarType.pvDouble).addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_SCALAR_BYTE: {
			valueField = fieldCreate.createStructureArray(
					NTScalar.createBuilder().value(ScalarType.pvByte).addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_SCALAR_SHORT: {
			valueField = fieldCreate.createStructureArray(
					NTScalar.createBuilder().value(ScalarType.pvShort).addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_SCALAR_ENUM: {
			// Not supported
		}
		case DBR_SCALAR_INT: {
			valueField = fieldCreate.createStructureArray(
					NTScalar.createBuilder().value(ScalarType.pvInt).addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_SCALAR_STRING: {
			valueField = fieldCreate.createStructureArray(
					NTScalar.createBuilder().value(ScalarType.pvString).addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_WAVEFORM_FLOAT: {
			valueField = fieldCreate.createStructureArray(NTScalarArray.createBuilder().value(ScalarType.pvString)
					.addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_WAVEFORM_DOUBLE: {
			valueField = fieldCreate.createStructureArray(NTScalarArray.createBuilder().value(ScalarType.pvDouble)
					.addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_WAVEFORM_SHORT: {
			valueField = fieldCreate.createStructureArray(
					NTScalarArray.createBuilder().value(ScalarType.pvShort).addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_WAVEFORM_BYTE: {
			valueField = fieldCreate.createStructureArray(
					NTScalarArray.createBuilder().value(ScalarType.pvByte).addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_WAVEFORM_INT: {
			valueField = fieldCreate.createStructureArray(
					NTScalarArray.createBuilder().value(ScalarType.pvInt).addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_WAVEFORM_STRING: {
			valueField = fieldCreate.createStructureArray(
					NTScalarArray.createBuilder().value(ScalarType.pvString).addAlarm().addTimeStamp().createStructure());
			break;
		}
		case DBR_WAVEFORM_ENUM:
		case DBR_V4_GENERIC_BYTES: {

		}
		default:
			throw new UnsupportedOperationException("Unknown DBR type " + typeInfo.getDBRType());
		}

		System.out.println("creating pvstructure for type " + typeInfo.getDBRType());
		Structure resultStructure = fieldCreate.createStructure("NTComplexTable",
				new String[] { "labels", "value" },
				new Field[] { fieldCreate.createScalarArray(ScalarType.pvString), valueField});
		
		return PVDataFactory.getPVDataCreate().createPVStructure(resultStructure);
	}

	public PVStructure getStruct() {
		return this.pvStruct;
	}


}
