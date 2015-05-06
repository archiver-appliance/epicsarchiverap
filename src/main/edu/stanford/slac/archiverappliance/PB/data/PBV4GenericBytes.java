package edu.stanford.slac.archiverappliance.PB.data;

import java.sql.Timestamp;
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
import org.epics.archiverappliance.data.ScalarStringSampleValue;
import org.epics.archiverappliance.engine.pv.EPICSV4.Data_EPICSV4;

import com.google.protobuf.ByteString;

import edu.stanford.slac.archiverappliance.PB.EPICSEvent;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.FieldValue;
import edu.stanford.slac.archiverappliance.PB.EPICSEvent.V4GenericBytes.Builder;
import edu.stanford.slac.archiverappliance.PB.utils.LineEscaper;

/**
 * A DBRTimeEvent that wraps a V4 struct. We store the val as a bunch of bytes
 * @author mshankar
 *
 */
public class PBV4GenericBytes implements DBRTimeEvent, PartionedTime {
	ByteArray bar = null;
	short year = 0;
	EPICSEvent.V4GenericBytes dbevent = null;

	public PBV4GenericBytes(short year, ByteArray bar) {
		this.bar = bar;
		this.year = year;
	}
	
	public PBV4GenericBytes(DBRTimeEvent ev) {
		YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(ev.getEventTimeStamp());
		year = yst.getYear();
		Builder builder = EPICSEvent.V4GenericBytes.newBuilder()
				.setSecondsintoyear(yst.getSecondsintoyear())
				.setNano(yst.getNanos())
				.setVal(ByteString.copyFromUtf8(ev.getSampleValue().toString()));
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
	
	public PBV4GenericBytes(Data_EPICSV4 dbr) {
		YearSecondTimestamp yst = TimeUtils.convertToYearSecondTimestamp(dbr.getTimeStamp());
		year = yst.getYear();
		Builder builder = EPICSEvent.V4GenericBytes.newBuilder()
				.setSecondsintoyear(yst.getSecondsintoyear())
				.setNano(yst.getNanos())
				.setVal(ByteString.copyFrom(dbr.getValue()));
		if(dbr.getSeverity() != 0) builder.setSeverity(dbr.getSeverity());
		if(dbr.getStatus() != 0) builder.setStatus(dbr.getStatus());
		dbevent = builder.build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));;
	}

	@Override
	public Event makeClone() {
		return new PBV4GenericBytes(this);
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
		return new ScalarStringSampleValue(dbevent.getVal().toStringUtf8());
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
		dbevent = EPICSEvent.V4GenericBytes.newBuilder().mergeFrom(dbevent).setRepeatcount(repeatCount).build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}

	@Override
	public void setStatus(int status) {
		unmarshallEventIfNull();
		if(status != 0) { 
			dbevent = EPICSEvent.V4GenericBytes.newBuilder().mergeFrom(dbevent).setStatus(status).build();
		} else { 
			dbevent = EPICSEvent.V4GenericBytes.newBuilder().mergeFrom(dbevent).build();
		}
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}
	
	@Override
	public void setSeverity(int severity) {
		unmarshallEventIfNull();
		if(severity != 0) { 
			dbevent = EPICSEvent.V4GenericBytes.newBuilder().mergeFrom(dbevent).setSeverity(severity).build();
		} else { 
			dbevent = EPICSEvent.V4GenericBytes.newBuilder().mergeFrom(dbevent).build();
		}
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}
	
	private void unmarshallEventIfNull() {
		try {
			if(dbevent == null) {
				dbevent = EPICSEvent.V4GenericBytes.newBuilder().mergeFrom(bar.inPlaceUnescape().unescapedData, bar.off, bar.unescapedLen).build();
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
		dbevent = EPICSEvent.V4GenericBytes.newBuilder().mergeFrom(dbevent).addFieldvalues(fv).build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}

	@Override
	public void markAsActualChange() {
		unmarshallEventIfNull();
		dbevent = EPICSEvent.V4GenericBytes.newBuilder().mergeFrom(dbevent).setFieldactualchange(true).build();
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
		dbevent = EPICSEvent.V4GenericBytes.newBuilder().mergeFrom(dbevent).addAllFieldvalues(fieldValuesList).setFieldactualchange(markAsActualChange).build();
		bar = new ByteArray(LineEscaper.escapeNewLines(dbevent.toByteArray()));
		return;
	}	

	@Override
	public ArchDBRTypes getDBRType() {
		return ArchDBRTypes.DBR_V4_GENERIC_BYTES;
	}
}
