package org.epics.archiverappliance.engine.pv.EPICSV4;

public enum DataType_EPICSV4 {

	TIME_INT, TIME_DOUBLE, TIME_FLOAT, TIME_LONG, TIME_STRING, TIME_BOOL, TIME_BYTE, TIME_SHORT,

	// For the moment , I think we just use TIME_VSTATIC_BYTE because we will
	// put value into byte[]
	TIME_VSTATIC_BYTES,
}
