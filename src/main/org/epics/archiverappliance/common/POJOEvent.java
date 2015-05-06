package org.epics.archiverappliance.common;

import java.sql.Timestamp;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;

/**
 * A simple POJO that implements the event interface.
 * Use for converting into the PB classes especially in the post processors.
 * @author mshankar
 *
 */
public class POJOEvent implements DBRTimeEvent {
	public static Logger logger = Logger.getLogger(POJOEvent.class.getName());
	private ArchDBRTypes dbrType;
	private Timestamp recordProcessingTime;
	private SampleValue sampleValue;
	private int status;
	private int severity;
	
	public POJOEvent(ArchDBRTypes dbrType, Timestamp recordProcessingTime, SampleValue sampleValue, int status, int severity) {
		super();
		this.dbrType = dbrType;
		this.recordProcessingTime = recordProcessingTime;
		this.sampleValue = sampleValue;
		this.status = status;
		this.severity = severity;
	}
	
	public POJOEvent(ArchDBRTypes dbrType, Timestamp recordProcessingTime, String sampleValueStr, int status, int severity) {
		this(dbrType, recordProcessingTime, ArchDBRTypes.sampleValueFromString(dbrType, sampleValueStr), status, severity);
	}

	
	@Override
	public Event makeClone() {
		try {
			return DBR2PBTypeMapping.getPBClassFor(dbrType).getSerializingConstructor().newInstance(this);
		} catch(Exception ex) {
			logger.error("Exception serializing POJO event into PB", ex);
			return null;
		}
	}


	@Override
	public SampleValue getSampleValue() {
		return sampleValue;
	}


	@Override
	public Timestamp getEventTimeStamp() {
		return recordProcessingTime;
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
	public long getEpochSeconds() {
		return TimeUtils.convertToEpochSeconds(recordProcessingTime);
	}

	@Override
	public ByteArray getRawForm() {
		try { 
			DBRTimeEvent ev = DBR2PBTypeMapping.getPBClassFor(dbrType).getSerializingConstructor().newInstance(this);
			return ev.getRawForm();
		} catch(Exception ex) {
			logger.error("Exception creating event object", ex);
			throw new RuntimeException("Unable to serialize a simulation event stream");
		}
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

	@Override
	public ArchDBRTypes getDBRType() {
		return this.dbrType;
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
