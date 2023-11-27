/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package edu.stanford.slac.archiverappliance.PB.data;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import gov.aps.jca.dbr.DBR;
import gov.aps.jca.dbr.DBR_TIME_Byte;
import gov.aps.jca.dbr.DBR_TIME_Double;
import gov.aps.jca.dbr.DBR_TIME_Enum;
import gov.aps.jca.dbr.DBR_TIME_Float;
import gov.aps.jca.dbr.DBR_TIME_Int;
import gov.aps.jca.dbr.DBR_TIME_Short;
import gov.aps.jca.dbr.DBR_TIME_String;

import java.util.Collections;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test storage of status and severity
 * @author mshankar
 *
 */
public class StatusSeverityTest {
	private static Logger logger = LogManager.getLogger(StatusSeverityTest.class.getName());


	@Test
	public void testJCAStatusAndSeverity() throws Exception {
		for(ArchDBRTypes dbrType : ArchDBRTypes.values()) {
			if(!dbrType.isV3Type()) continue;
			logger.info("Testing JCA status and severity for DBR_type: " + dbrType.name());
			for(int severity = 0; severity < 4; severity++) {
				for(int status = 0; status < 22; status++) {
					try {
						DBR dbr = getJCASampleValue(dbrType, 0, severity, status);
						DBRTimeEvent e = (DBRTimeEvent) EPICS2PBTypeMapping.getPBClassFor(dbrType).getJCADBRConstructor().newInstance(dbr);
						assertTrue("Severities are different " + e.getSeverity() + " and " + severity, e.getSeverity() == severity);
						assertTrue("Statuses are different " + e.getStatus() + " and " + status, e.getStatus() == status);
					} catch(Exception ex) {
						logger.error("Exception for severity " + severity + " and status " + status, ex);
						fail("Exception for severity " + severity + " and status " + status);
					}
				}						
			}
		}
	}
	
	
	private DBR getJCASampleValue(ArchDBRTypes type, int value, int severity, int status) {
		switch(type) {
		case DBR_SCALAR_STRING:
			DBR_TIME_String retvalss = new DBR_TIME_String(new String[] { Integer.toString(value) });
			retvalss.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvalss.setSeverity(severity);
			retvalss.setStatus(status);
			return retvalss;
		case DBR_SCALAR_SHORT:
			DBR_TIME_Short retvalsh;
			if(0 <= value && value < 1000) {
				// Check for some numbers around the minimum value
				retvalsh = new DBR_TIME_Short(new short[] { (short) (Short.MIN_VALUE + value) } );
			} else if (1000 <= value && value < 2000) {
				// Check for some numbers around the maximum value
				retvalsh =  new DBR_TIME_Short(new short[] { (short) (Short.MAX_VALUE - (value-1000)) } );
			} else {
				// Check for some numbers around 0
				retvalsh =  new DBR_TIME_Short(new short[] { (short) (value - 2000) } );
			}
			retvalsh.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvalsh.setSeverity(severity);
			retvalsh.setStatus(status);
			return retvalsh;
		case DBR_SCALAR_FLOAT:
			DBR_TIME_Float retvalfl;
			if(0 <= value && value < 1000) {
				// Check for some numbers around the minimum value
				retvalfl = new DBR_TIME_Float(new float[] { Float.MIN_VALUE + value } );
			} else if (1000 <= value && value < 2000) {
				// Check for some numbers around the maximum value
				retvalfl = new DBR_TIME_Float(new float[] { Float.MAX_VALUE - (value-1000) } );
			} else {
				// Check for some numbers around 0. Divide by a large number to make sure we cater to the number of precision digits
				retvalfl = new DBR_TIME_Float(new float[] { (value - 2000.0f)/value } );
			}
			retvalfl.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvalfl.setSeverity(severity);
			retvalfl.setStatus(status);
			return retvalfl;
		case DBR_SCALAR_ENUM:
			DBR_TIME_Enum retvalen;
			retvalen = new DBR_TIME_Enum(new short[] { (short) (value) } );
			retvalen.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvalen.setSeverity(severity);
			retvalen.setStatus(status);
			return retvalen;
		case DBR_SCALAR_BYTE:
			DBR_TIME_Byte retvalby;
			retvalby = new DBR_TIME_Byte(new byte[] { ((byte)(value%255)) } );
			retvalby.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvalby.setSeverity(severity);
			retvalby.setStatus(status);
			return retvalby;
		case DBR_SCALAR_INT:
			DBR_TIME_Int retvalint;
			if(0 <= value && value < 1000) {
				// Check for some numbers around the minimum value
				retvalint = new DBR_TIME_Int(new int[] { Integer.MIN_VALUE + value } );
			} else if (1000 <= value && value < 2000) {
				// Check for some numbers around the maximum value
				retvalint = new DBR_TIME_Int(new int[] { Integer.MAX_VALUE - (value-1000) } );
			} else {
				// Check for some numbers around 0
				retvalint = new DBR_TIME_Int(new int[] { (value - 2000) } );
			}
			retvalint.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvalint.setSeverity(severity);
			retvalint.setStatus(status);
			return retvalint;
		case DBR_SCALAR_DOUBLE:
			DBR_TIME_Double retvaldb;
			if(0 <= value && value < 1000) {
				// Check for some numbers around the minimum value
				retvaldb = new DBR_TIME_Double(new double[] { (Double.MIN_VALUE + value) } );
			} else if (1000 <= value && value < 2000) {
				// Check for some numbers around the maximum value
				retvaldb = new DBR_TIME_Double(new double[] { (Double.MAX_VALUE - (value-1000)) } );
			} else {
				// Check for some numbers around 0. Divide by a large number to make sure we cater to the number of precision digits
				retvaldb = new DBR_TIME_Double(new double[] { ((value - 2000.0)/(value*1000000)) } );
			}
			retvaldb.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvaldb.setSeverity(severity);
			retvaldb.setStatus(status);
			return retvaldb;
		case DBR_WAVEFORM_STRING:
			DBR_TIME_String retvst;
			// Varying number of copies of a typical value
			retvst = new DBR_TIME_String(Collections.nCopies(value, Integer.toString(value)).toArray(new String[0]));
			retvst.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvst.setSeverity(severity);
			retvst.setStatus(status);
			return retvst;
		case DBR_WAVEFORM_SHORT:
			DBR_TIME_Short retvsh;
			retvsh = new DBR_TIME_Short(ArrayUtils.toPrimitive(Collections.nCopies(1, (short) value).toArray(new Short[0])));
			retvsh.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvsh.setSeverity(severity);
			retvsh.setStatus(status);
			return retvsh;
		case DBR_WAVEFORM_FLOAT:
			DBR_TIME_Float retvf;
			// Varying number of copies of a typical value
			retvf = new DBR_TIME_Float(ArrayUtils.toPrimitive(Collections.nCopies(value, (float) Math.cos(value*Math.PI/3600)).toArray(new Float[0])));
			retvf.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvf.setSeverity(severity);
			retvf.setStatus(status);
			return retvf;
		case DBR_WAVEFORM_ENUM:
			DBR_TIME_Enum retven;
			retven = new DBR_TIME_Enum(ArrayUtils.toPrimitive(Collections.nCopies(1024, (short) value).toArray(new Short[0])));
			retven.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retven.setSeverity(severity);
			retven.setStatus(status);
			return retven;
		case DBR_WAVEFORM_BYTE:
			DBR_TIME_Byte retvb;
			// Large number of elements in the array
			retvb = new DBR_TIME_Byte(ArrayUtils.toPrimitive(Collections.nCopies(65536*value, ((byte)(value%255))).toArray(new Byte[0])));
			retvb.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvb.setSeverity(severity);
			retvb.setStatus(status);
			return retvb;
		case DBR_WAVEFORM_INT:
			DBR_TIME_Int retvint;
			// Varying number of copies of a typical value
			retvint = new DBR_TIME_Int(ArrayUtils.toPrimitive(Collections.nCopies(value, value*value).toArray(new Integer[0])));
			retvint.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvint.setSeverity(severity);
			retvint.setStatus(status);
			return retvint;
		case DBR_WAVEFORM_DOUBLE:
			DBR_TIME_Double retvd;
			// Varying number of copies of a typical value
			retvd = new DBR_TIME_Double(ArrayUtils.toPrimitive(Collections.nCopies(value, Math.sin(value*Math.PI/3600)).toArray(new Double[0])));
			retvd.setTimeStamp(convertSecondsIntoYear2JCATimeStamp(value));
			retvd.setSeverity(severity);
			retvd.setStatus(status);
			return retvd;
		case DBR_V4_GENERIC_BYTES:
			throw new RuntimeException("Currently don't support " + type + " when generating sample data");			
		default: 
			throw new RuntimeException("We seemed to have missed a DBR type when generating sample data");
		}
	}
	
	private static gov.aps.jca.dbr.TimeStamp convertSecondsIntoYear2JCATimeStamp(int secondsintoYear) {
		return new gov.aps.jca.dbr.TimeStamp(TimeUtils.getStartOfCurrentYearInSeconds() + secondsintoYear);
	}

}
