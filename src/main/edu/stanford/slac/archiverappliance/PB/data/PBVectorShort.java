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
import org.epics.archiverappliance.data.DBRAlarm;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;
import org.epics.archiverappliance.data.VectorValue;
import org.epics.pva.data.PVAShortArray;
import org.epics.pva.data.PVAStructure;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.FieldValue;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.VectorShort.Builder;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBR_TIME_Short;

/**
 * A DBRTimeEvent for a vector short. 
 * @author mshankar
 *
 */
public class PBVectorShort implements DBRTimeEvent, PartionedTime {
	ByteArray bar = null;
	short year = 0;
	EPICSEvent.VectorShort dbevent = null;

	public PBVectorShort(short year, ByteArray bar) {
		this.bar = bar;
		this.year = year;
	}
	
	@SuppressWarnings("unchecked")
	public PBVectorShort(DBRTimeEvent ev) {
		List<Short> srcvals = (List<Short>) ev.getSampleValue().getValues();
		ArrayList<Integer> vals = new ArrayList<Integer>(srcvals.size());
		for(int val : srcvals) vals.add(val);
		YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(ev.getEventTimeStamp());
		year = yst.getYear();
		Builder builder = EPICSEvent.VectorShort.newBuilder()
				.setSecondsintoyear(yst.getSecondsintoyear())
				.setNano(yst.getNanos())
				.addAllVal(vals);
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

	public PBVectorShort(DBR dbr) {
		DBR_TIME_Short realtype = (DBR_TIME_Short) dbr;
		YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(realtype.getTimeStamp());
		year = yst.getYear();
		ArrayList<Integer> vals = new ArrayList<Integer>(realtype.getCount());
		for(int val : realtype.getShortValue()) vals.add(val);
		Builder builder = EPICSEvent.VectorShort.newBuilder()
				.setSecondsintoyear(yst.getSecondsintoyear())
				.setNano(yst.getNanos())
				.addAllVal(vals);
		if(realtype.getSeverity().getValue() != 0) builder.setSeverity(realtype.getSeverity().getValue());
		if(realtype.getStatus().getValue() != 0) builder.setStatus(realtype.getStatus().getValue());
		dbevent = builder.build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
	}

	public PBVectorShort(PVAStructure v4Data) {
        YearSecondTimestamp yst = TimeUtils.convertFromPVTimeStamp(v4Data.get("timeStamp"));
		DBRAlarm alarm = DBRAlarm.convertPVAlarm(v4Data.get("alarm"));

		PVAShortArray pvArray = v4Data.get("value");
		ArrayList<Integer> vals = new ArrayList<Integer>();
		short[] data = pvArray.get();
		for(short dataitem : data) { 
			vals.add((int) dataitem);
		}

		year = yst.getYear();
		Builder builder = EPICSEvent.VectorShort.newBuilder()
				.setSecondsintoyear(yst.getSecondsintoyear())
				.setNano(yst.getNanos())
				.addAllVal(vals);
		if(alarm.severity != 0) builder.setSeverity(alarm.severity);
		if(alarm.status != 0) builder.setStatus(alarm.status);
		dbevent = builder.build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
	}
	
	@Override
	public Event makeClone() {
		return new PBVectorShort(this);
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
		List<Short> shortvals = new ArrayList<Short>(dbevent.getValList().size());
		for(int val : dbevent.getValList()) shortvals.add((short) val);
		return new VectorValue<Short>(shortvals);
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
		dbevent = EPICSEvent.VectorShort.newBuilder().mergeFrom(dbevent).setRepeatcount(repeatCount).build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}

	@Override
	public void setStatus(int status) {
		unmarshallEventIfNull();
		if(status != 0) { 
			dbevent = EPICSEvent.VectorShort.newBuilder().mergeFrom(dbevent).setStatus(status).build();
		} else { 
			dbevent = EPICSEvent.VectorShort.newBuilder().mergeFrom(dbevent).build();
		}
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}
	
	@Override
	public void setSeverity(int severity) {
		unmarshallEventIfNull();
		if(severity != 0) { 
			dbevent = EPICSEvent.VectorShort.newBuilder().mergeFrom(dbevent).setSeverity(severity).build();
		} else { 
			dbevent = EPICSEvent.VectorShort.newBuilder().mergeFrom(dbevent).build();
		}
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}
	
	private void unmarshallEventIfNull() {
		try {
			if(dbevent == null) {
				dbevent = EPICSEvent.VectorShort.newBuilder().mergeFrom(bar.inPlaceUnescape().unescapedData, bar.off, bar.unescapedLen).build();
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
		dbevent = EPICSEvent.VectorShort.newBuilder().mergeFrom(dbevent).addFieldvalues(fv).build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}

	@Override
	public void markAsActualChange() {
		unmarshallEventIfNull();
		dbevent = EPICSEvent.VectorShort.newBuilder().mergeFrom(dbevent).setFieldactualchange(true).build();
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
		dbevent = EPICSEvent.VectorShort.newBuilder().mergeFrom(dbevent).addAllFieldvalues(fieldValuesList).setFieldactualchange(markAsActualChange).build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}	

	@Override
	public ArchDBRTypes getDBRType() {
		return ArchDBRTypes.DBR_WAVEFORM_SHORT;
	}
}
