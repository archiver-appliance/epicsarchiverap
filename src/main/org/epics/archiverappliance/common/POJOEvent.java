package org.epics.archiverappliance.common;

import edu.stanford.slac.archiverappliance.PB.data.DBR2PBTypeMapping;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.ByteArray;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.data.SampleValue;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.HashMap;

/**
 * A simple POJO that implements the event interface.
 * Use for converting into the PB classes especially in the post processors.
 * @author mshankar
 *
 */
public class POJOEvent implements DBRTimeEvent {
	public static Logger logger = LogManager.getLogger(POJOEvent.class.getName());
	private ArchDBRTypes dbrType;
	private Instant recordProcessingTime;
	private SampleValue sampleValue;
	private int status;
	private int severity;

	public POJOEvent(ArchDBRTypes dbrType, Instant recordProcessingTime, SampleValue sampleValue, int status, int severity) {
		super();
		this.dbrType = dbrType;
		this.recordProcessingTime = recordProcessingTime;
		this.sampleValue = sampleValue;
		this.status = status;
		this.severity = severity;
	}

	public POJOEvent(ArchDBRTypes dbrType, Instant recordProcessingTime, String sampleValueStr, int status, int severity) {
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
	public Instant getEventTimeStamp() {
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
	}


	@Override
	public long getEpochSeconds() {
		return TimeUtils.convertToEpochSeconds(recordProcessingTime);
	}

	@Override
	public ByteArray getRawForm() {
		DBRTimeEvent ev = getDbrTimeEvent();
		return ev.getRawForm();
	}


	private DBRTimeEvent getDbrTimeEvent() {
		DBRTimeEvent ev = null;
		try {
			ev = DBR2PBTypeMapping.getPBClassFor(dbrType).getSerializingConstructor().newInstance(this);
		} catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {

			logger.error("Exception creating event object", e);
			throw new RuntimeException("Unable to serialize a simulation event stream");
		}
		return ev;
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
