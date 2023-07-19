/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.config;

import static org.junit.Assert.assertSame;
import gov.aps.jca.dbr.DBR_TIME_Byte;
import gov.aps.jca.dbr.DBR_TIME_Double;
import gov.aps.jca.dbr.DBR_TIME_Enum;
import gov.aps.jca.dbr.DBR_TIME_Float;
import gov.aps.jca.dbr.DBR_TIME_Int;
import gov.aps.jca.dbr.DBR_TIME_Short;
import gov.aps.jca.dbr.DBR_TIME_String;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test the mapping for ArchDBRTypes
 * @author mshankar
 *
 */
public class ArchDBRTypesTest {


	@Test
	public void testValueOf() {
		JCA2ArchDBRType.values(); // Initialize the enum?
		assertSame(ArchDBRTypes.DBR_SCALAR_STRING.toString(), ArchDBRTypes.DBR_SCALAR_STRING, JCA2ArchDBRType.valueOf(new DBR_TIME_String()));
		assertSame(ArchDBRTypes.DBR_SCALAR_SHORT.toString(), ArchDBRTypes.DBR_SCALAR_SHORT, JCA2ArchDBRType.valueOf(new DBR_TIME_Short()));
		assertSame(ArchDBRTypes.DBR_SCALAR_FLOAT.toString(), ArchDBRTypes.DBR_SCALAR_FLOAT, JCA2ArchDBRType.valueOf(new DBR_TIME_Float()));
		assertSame(ArchDBRTypes.DBR_SCALAR_ENUM.toString(), ArchDBRTypes.DBR_SCALAR_ENUM, JCA2ArchDBRType.valueOf(new DBR_TIME_Enum()));
		assertSame(ArchDBRTypes.DBR_SCALAR_BYTE.toString(), ArchDBRTypes.DBR_SCALAR_BYTE, JCA2ArchDBRType.valueOf(new DBR_TIME_Byte()));
		assertSame(ArchDBRTypes.DBR_SCALAR_INT.toString(), ArchDBRTypes.DBR_SCALAR_INT, JCA2ArchDBRType.valueOf(new DBR_TIME_Int()));
		assertSame(ArchDBRTypes.DBR_SCALAR_DOUBLE.toString(), ArchDBRTypes.DBR_SCALAR_DOUBLE, JCA2ArchDBRType.valueOf(new DBR_TIME_Double()));

		assertSame(ArchDBRTypes.DBR_WAVEFORM_STRING.toString(), ArchDBRTypes.DBR_WAVEFORM_STRING, JCA2ArchDBRType.valueOf(new DBR_TIME_String(2)));
		assertSame(ArchDBRTypes.DBR_WAVEFORM_SHORT.toString(), ArchDBRTypes.DBR_WAVEFORM_SHORT, JCA2ArchDBRType.valueOf(new DBR_TIME_Short(2)));
		assertSame(ArchDBRTypes.DBR_WAVEFORM_FLOAT.toString(), ArchDBRTypes.DBR_WAVEFORM_FLOAT, JCA2ArchDBRType.valueOf(new DBR_TIME_Float(2)));
		assertSame(ArchDBRTypes.DBR_WAVEFORM_ENUM.toString(), ArchDBRTypes.DBR_WAVEFORM_ENUM, JCA2ArchDBRType.valueOf(new DBR_TIME_Enum(2)));
		assertSame(ArchDBRTypes.DBR_WAVEFORM_BYTE.toString(), ArchDBRTypes.DBR_WAVEFORM_BYTE, JCA2ArchDBRType.valueOf(new DBR_TIME_Byte(2)));
		assertSame(ArchDBRTypes.DBR_WAVEFORM_INT.toString(), ArchDBRTypes.DBR_WAVEFORM_INT, JCA2ArchDBRType.valueOf(new DBR_TIME_Int(2)));
		assertSame(ArchDBRTypes.DBR_WAVEFORM_DOUBLE.toString(), ArchDBRTypes.DBR_WAVEFORM_DOUBLE, JCA2ArchDBRType.valueOf(new DBR_TIME_Double(2)));
		
		// TODO - Fix once we have upgraded the EPICS V4 jars.
		// assertSame(ArchDBRTypes.DBR_V4_GENERIC_BYTES.toString(), ArchDBRTypes.DBR_V4_GENERIC_BYTES, EPICSV42DBRType.valueOf(DataType_EPICSV4.TIME_VSTATIC_BYTES));

	}

}
