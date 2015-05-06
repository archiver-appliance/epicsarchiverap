package org.epics.archiverappliance.engine.pv.EPICSV4;

import gov.aps.jca.dbr.TimeStamp;

public class Data_EPICSV4 {
	private TimeStamp timeStamp;
	private int severity;
	private int status;

	// value is byte type I use string value="1.0"+"   2.0"
	// first 1.0 is the pahse
	// second 2.0 is the amplitude
	private byte[] value;

	public Data_EPICSV4(TimeStamp timeStamp, int severity, int status,
			byte[] value) {
		this.timeStamp = timeStamp;
		this.severity = severity;
		this.status = status;
		this.value = value;
	}

	public TimeStamp getTimeStamp() {
		return timeStamp;
	}

	public int getSeverity() {
		return severity;
	}

	public int getStatus() {
		return status;
	}

	public byte[] getValue() {
		return value;
	}

}
