/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.common.YearSecondTimestamp;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.VectorValue;
import org.epics.pvdata.pv.ByteArrayData;
import org.epics.pvdata.pv.PVByteArray;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;

import com.google.protobuf.ByteString;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.FieldValue;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.VectorChar.Builder;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBR_TIME_Byte;

/**
 * A DBRTimeEvent for a vector byte. 
 * @author mshankar
 *
 */
public class PBVectorByte implements DBRTimeEvent, PartionedTime {
	ByteArray bar = null;
	short year = 0;
	EPICSEvent.VectorChar dbevent = null;

	
	public PBVectorByte(short year, ByteArray bar) {
		this.bar = bar;
		this.year = year;
	}
	
	@SuppressWarnings("unchecked")
	public PBVectorByte(DBRTimeEvent ev) {
		List<Byte> bytes = ev.getSampleValue().getValues();
		byte[] vals = new byte[bytes.size()];
		int i = 0;
		for(Byte b : bytes) {
			vals[i++] = b;
		}
		YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(ev.getEventTimeStamp());
		year = yst.getYear();
		Builder builder = EPICSEvent.VectorChar.newBuilder()
				.setSecondsintoyear(yst.getSecondsintoyear())
				.setNano(yst.getNanos())
				.setVal(ByteString.copyFrom(vals));
		if(ev.getSeverity() != 0) builder.setSeverity(ev.getSeverity());
		if(ev.getStatus() != 0) builder.setStatus(ev.getStatus());
		if(ev.hasFieldValues()) {
			HashMap<String, String> fields = ev.getFields();
			for(String fieldName : fields.keySet()) {
				FieldValue fv = EPICSEvent.FieldValue.newBuilder().setName(fieldName).setVal(fields.get(fieldName)).build();
				builder.addFieldvalues(fv);
			}
			builder.setFieldactualchange(ev.isActualChange());
		}
		dbevent = builder.build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
	}

	public PBVectorByte(DBR dbr) {
		DBR_TIME_Byte realtype = (DBR_TIME_Byte) dbr;
		byte[] vals = new byte[realtype.getCount()];
		int i = 0;
		for(Byte b : realtype.getByteValue()) {
			vals[i++] = b;
		}
		YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(realtype.getTimeStamp());
		year = yst.getYear();
		Builder builder = EPICSEvent.VectorChar.newBuilder()
				.setSecondsintoyear(yst.getSecondsintoyear())
				.setNano(yst.getNanos())
				.setVal(ByteString.copyFrom(vals));
		if(realtype.getSeverity().getValue() != 0) builder.setSeverity(realtype.getSeverity().getValue());
		if(realtype.getStatus().getValue() != 0) builder.setStatus(realtype.getStatus().getValue());
		dbevent = builder.build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));;
	}

	public PBVectorByte(PVStructure v4Data) {
		PVStructure timeStampPVStructure = v4Data.getStructureField("timeStamp");
		long secondsPastEpoch = timeStampPVStructure.getLongField("secondsPastEpoch").get();
		int nanoSeconds = timeStampPVStructure.getIntField("nanoseconds").get();
		Timestamp timestamp = TimeUtils.convertFromEpochSeconds(secondsPastEpoch, nanoSeconds);
		YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(timestamp);

		PVStructure alarmPVStructure = v4Data.getStructureField("alarm");
		int severity = alarmPVStructure.getIntField("severity").get();
		int status = alarmPVStructure.getIntField("status").get();

		PVByteArray pvArray = (PVByteArray) v4Data.getScalarArrayField("value", ScalarType.pvByte);
		ByteArrayData arrayData = new ByteArrayData();
		pvArray.get(0, pvArray.getLength(), arrayData);
		byte[] data = arrayData.data;

		year = yst.getYear();
		Builder builder = EPICSEvent.VectorChar.newBuilder()
				.setSecondsintoyear(yst.getSecondsintoyear())
				.setNano(yst.getNanos())
				.setVal(ByteString.copyFrom(data));
		if(severity != 0) builder.setSeverity(severity);
		if(status != 0) builder.setStatus(status);
		dbevent = builder.build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
	}

	@Override
	public Event makeClone() {
		return new PBVectorByte(this);
	}

	
	@Override
	public Timestamp getEventTimeStamp() {
		unmarshallEventIfNull();
		return TimeUtils.convertFromYearSecondTimestamp(new YearSecondTimestamp(year, dbevent.getSecondsintoyear(), dbevent.getNano()));
	}
	
	@Override
	public short getYear() {
		return year;
	}
	
	@Override
	public int getSecondsIntoYear() {
		unmarshallEventIfNull();
		return dbevent.getSecondsintoyear();
	}

	@Override
	public long getEpochSeconds() {
		unmarshallEventIfNull();
		return TimeUtils.getStartOfYearInSeconds(year) + dbevent.getSecondsintoyear();
	}

	@Override
	public ByteArray getRawForm() {
		return bar;
	}

	@Override
	public SampleValue getSampleValue() {
		unmarshallEventIfNull();
		byte[] bytes = dbevent.getVal().toByteArray();
		ArrayList<Byte> ret = new ArrayList<Byte>(bytes.length);
		for(byte b : bytes) {
			ret.add(new Byte(b));
		}
		return new VectorValue<Byte>(ret);
	}

	@Override
	public int getStatus() {
		unmarshallEventIfNull();
		return dbevent.getStatus();
	}

	@Override
	public int getSeverity() {
		unmarshallEventIfNull();
		return dbevent.getSeverity();
	}

	@Override
	public int getRepeatCount() {
		unmarshallEventIfNull();
		return dbevent.getRepeatcount();
	}
	
	@Override
	public void setRepeatCount(int repeatCount) {
		unmarshallEventIfNull();
		dbevent = EPICSEvent.VectorChar.newBuilder().mergeFrom(dbevent).setRepeatcount(repeatCount).build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}

	@Override
	public void setStatus(int status) {
		unmarshallEventIfNull();
		if(status != 0) { 
			dbevent = EPICSEvent.VectorChar.newBuilder().mergeFrom(dbevent).setStatus(status).build();
		} else { 
			dbevent = EPICSEvent.VectorChar.newBuilder().mergeFrom(dbevent).build();
		}
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}
	
	@Override
	public void setSeverity(int severity) {
		unmarshallEventIfNull();
		if(severity != 0) { 
			dbevent = EPICSEvent.VectorChar.newBuilder().mergeFrom(dbevent).setSeverity(severity).build();
		} else { 
			dbevent = EPICSEvent.VectorChar.newBuilder().mergeFrom(dbevent).build();
		}
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}
	
	private void unmarshallEventIfNull() {
		try {
			if(dbevent == null) {
				dbevent = EPICSEvent.VectorChar.newBuilder().mergeFrom(bar.inPlaceUnescape().unescapedData, bar.off, bar.unescapedLen).build();
			}
		} catch(Exception ex) {
			throw new PBParseException(bar.toBytes(), ex);
		}
	}
	@Override
	public boolean hasFieldValues() {
		unmarshallEventIfNull();
		return dbevent.getFieldvaluesCount() > 0;
	}

	@Override
	public boolean isActualChange() {
		unmarshallEventIfNull();
		return dbevent.getFieldactualchange();
	}

	@Override
	public HashMap<String, String> getFields() {
		unmarshallEventIfNull();
		HashMap<String, String> ret = new HashMap<String, String>();
		List<FieldValue> fieldValues = dbevent.getFieldvaluesList();
		if(fieldValues != null && !fieldValues.isEmpty()) {
			for(FieldValue fieldValue : fieldValues) {
				ret.put(fieldValue.getName(), fieldValue.getVal());
			}
		}
		return ret;
	}

	@Override
	public String getFieldValue(String fieldName) {
		unmarshallEventIfNull();
		List<FieldValue> fieldValues = dbevent.getFieldvaluesList();
		if(fieldValues != null && !fieldValues.isEmpty()) {
			for(FieldValue fieldValue : fieldValues) {
				if(fieldValue.getName().equals(fieldName)) {
					return fieldValue.getVal();
				}
			}
		}
		return null;
	}

	@Override
	public void addFieldValue(String fieldName, String fieldValue) {
		unmarshallEventIfNull();
		FieldValue fv = EPICSEvent.FieldValue.newBuilder().setName(fieldName).setVal(fieldValue).build();
		dbevent = EPICSEvent.VectorChar.newBuilder().mergeFrom(dbevent).addFieldvalues(fv).build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}

	@Override
	public void markAsActualChange() {
		unmarshallEventIfNull();
		dbevent = EPICSEvent.VectorChar.newBuilder().mergeFrom(dbevent).setFieldactualchange(true).build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}

	@Override
	public void setFieldValues(HashMap<String, String> fieldValues, boolean markAsActualChange) {
		unmarshallEventIfNull();
		LinkedList<FieldValue> fieldValuesList = new LinkedList<FieldValue>();
		for(String fieldName : fieldValues.keySet()) {
			fieldValuesList.add(EPICSEvent.FieldValue.newBuilder().setName(fieldName).setVal(fieldValues.get(fieldName)).build());
		}
		dbevent = EPICSEvent.VectorChar.newBuilder().mergeFrom(dbevent).addAllFieldvalues(fieldValuesList).setFieldactualchange(markAsActualChange).build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}	

	@Override
	public ArchDBRTypes getDBRType() {
		return ArchDBRTypes.DBR_WAVEFORM_BYTE;
	}
}
