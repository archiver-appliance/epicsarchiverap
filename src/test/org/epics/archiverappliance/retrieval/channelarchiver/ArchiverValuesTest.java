/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.channelarchiver;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Simple unit test using a sample from LCLS production for DBR_SCALAR_DOUBLE.
 * @author mshankar
 *
 */
public class ArchiverValuesTest {
	private static Logger logger = LogManager.getLogger(ArchiverValuesTest.class.getName());

	@BeforeEach
	public void setUp() throws Exception {
	}

	@AfterEach
	public void tearDown() throws Exception {
	}

	@Test
	public void testDBR_SCALAR_DOUBLE() throws Exception {
		File f = new File("src/test/org/epics/archiverappliance/retrieval/channelarchiver/DBR_SCALAR_DOUBLE_values.xml");
		logger.info("Testing using " + f.getAbsolutePath());
		FileInputStream is = new FileInputStream(f);
		try(ArchiverValuesHandler handler = new ArchiverValuesHandler("DummyPVName", is, f.toString(), null)) { 
			HashMap<String, String> metaInfo = handler.getMetaInformation();
			for(String key : handler.getMetaInformation().keySet()) {
				logger.debug(key + "=" + handler.getMetaInformation().get(key));
			}
			Assertions.assertTrue((metaInfo.get("disp_low") != null), "Could not determine " + "disp_low");
			Assertions.assertTrue((metaInfo.get("disp_high") != null), "Could not determine " + "disp_high");
			Assertions.assertTrue((metaInfo.get("alarm_low") != null), "Could not determine " + "alarm_low");
			Assertions.assertTrue((metaInfo.get("alarm_high") != null), "Could not determine " + "alarm_high");
			Assertions.assertTrue((metaInfo.get("warn_high") != null), "Could not determine " + "warn_high");
			Assertions.assertTrue((metaInfo.get("warn_low") != null), "Could not determine " + "warn_low");
			Assertions.assertTrue((metaInfo.get("units") != null), "Could not determine " + "units");
			Assertions.assertTrue((metaInfo.get("prec") != null), "Could not determine " + "prec");

			Assertions.assertTrue(handler.getValueType().getDBRType(handler.getElementCount()) == ArchDBRTypes.DBR_SCALAR_DOUBLE, "Type is " + handler.getValueType().getDBRType(handler.getElementCount()) + " expecting DBR_DOUBLE");
			Assertions.assertTrue(handler.getElementCount() == 1, "Element Count is " + handler.getElementCount());

			int expectedEventCount = 100;
			int eventCount = 0;
			for(Event event : handler) {
				StringBuilder eventStr = new StringBuilder();
				eventStr.append(event.getEpochSeconds() + "," + event.getSampleValue().toString());
				logger.debug(eventStr.toString());
				eventCount++;
			}

			Assertions.assertTrue(eventCount == expectedEventCount, "Expected " + expectedEventCount + " and got " + eventCount);
		}
	}
	
	
	@Test
	public void testDBR_WAVEFORM_DOUBLE() throws Exception {
		File f = new File("src/test/org/epics/archiverappliance/retrieval/channelarchiver/DBR_WAVEFORM_DOUBLE_values.xml");
		logger.info("Testing using " + f.getAbsolutePath());
		FileInputStream is = new FileInputStream(f);
		try(ArchiverValuesHandler handler = new ArchiverValuesHandler("DummyPVName", is, f.toString(), null)) { 
			HashMap<String, String> metaInfo = handler.getMetaInformation();
			for(String key : handler.getMetaInformation().keySet()) {
				logger.debug(key + "=" + handler.getMetaInformation().get(key));
			}
			Assertions.assertTrue((metaInfo.get("disp_low") != null), "Could not determine " + "disp_low");
			Assertions.assertTrue((metaInfo.get("disp_high") != null), "Could not determine " + "disp_high");
			Assertions.assertTrue((metaInfo.get("alarm_low") != null), "Could not determine " + "alarm_low");
			Assertions.assertTrue((metaInfo.get("alarm_high") != null), "Could not determine " + "alarm_high");
			Assertions.assertTrue((metaInfo.get("warn_high") != null), "Could not determine " + "warn_high");
			Assertions.assertTrue((metaInfo.get("warn_low") != null), "Could not determine " + "warn_low");
			Assertions.assertTrue((metaInfo.get("units") != null), "Could not determine " + "units");
			Assertions.assertTrue((metaInfo.get("prec") != null), "Could not determine " + "prec");

			Assertions.assertTrue(handler.getValueType().getDBRType(handler.getElementCount()) == ArchDBRTypes.DBR_WAVEFORM_DOUBLE, "Type is " + handler.getValueType().getDBRType(handler.getElementCount()) + " expecting DBR_WAVEFORM_DOUBLE");
			Assertions.assertTrue(handler.getElementCount() > 1, "Element Count is " + handler.getElementCount());

			int expectedEventCount = 5;
			int eventCount = 0;
			for(Event event : handler) {
				StringBuilder eventStr = new StringBuilder();
				eventStr.append(event.getEpochSeconds() + "," + event.getSampleValue().toString());
				logger.debug(eventStr.toString());
				Assertions.assertTrue((event.getSampleValue().getElementCount() == 7200), "Invalid event count, we got " + event.getSampleValue().getElementCount());
				logger.info("We got " + event.getSampleValue().getElementCount() + " events.");
				eventCount++;
			}

			Assertions.assertTrue(eventCount == expectedEventCount, "Expected " + expectedEventCount + " and got " + eventCount);
		}
	}
	
	
	@Test
	public void testNOData() throws Exception {
		File f = new File("src/test/org/epics/archiverappliance/retrieval/channelarchiver/NODATA_Sample.xml");
		logger.info("Testing using " + f.getAbsolutePath());
		FileInputStream is = new FileInputStream(f);
		try(ArchiverValuesHandler handler = new ArchiverValuesHandler("DummyPVName", is, f.toString(), null)) { 
			HashMap<String, String> metaInfo = handler.getMetaInformation();
			for(String key : handler.getMetaInformation().keySet()) {
				logger.debug(key + "=" + handler.getMetaInformation().get(key));
			}
			Assertions.assertTrue((metaInfo.get("disp_low") != null), "Could not determine " + "disp_low");
			Assertions.assertTrue((metaInfo.get("disp_high") != null), "Could not determine " + "disp_high");
			Assertions.assertTrue((metaInfo.get("alarm_low") != null), "Could not determine " + "alarm_low");
			Assertions.assertTrue((metaInfo.get("alarm_high") != null), "Could not determine " + "alarm_high");
			Assertions.assertTrue((metaInfo.get("warn_high") != null), "Could not determine " + "warn_high");
			Assertions.assertTrue((metaInfo.get("warn_low") != null), "Could not determine " + "warn_low");
			Assertions.assertTrue((metaInfo.get("units") != null), "Could not determine " + "units");
			Assertions.assertTrue((metaInfo.get("prec") != null), "Could not determine " + "prec");

			Assertions.assertTrue(handler.getValueType().getDBRType(handler.getElementCount()) == ArchDBRTypes.DBR_SCALAR_ENUM, "Type is " + handler.getValueType().getDBRType(handler.getElementCount()) + " expecting DBR_SCALAR_ENUM");
			Assertions.assertTrue(handler.getElementCount() == 1, "Element Count is " + handler.getElementCount());

			int expectedEventCount = 0;
			int eventCount = 0;
			for(Event event : handler) {
				StringBuilder eventStr = new StringBuilder();
				eventStr.append(event.getEpochSeconds() + "," + event.getSampleValue().toString());
				logger.debug(eventStr.toString());
				eventCount++;
			}

			Assertions.assertTrue(eventCount == expectedEventCount, "Expected " + expectedEventCount + " and got " + eventCount);
		}
	}
	
	@Test
	public void testTwoElementsTest() throws Exception {
		File f = new File("src/test/org/epics/archiverappliance/retrieval/channelarchiver/TwoElementsTest.xml");
		logger.info("Testing using " + f.getAbsolutePath());
		FileInputStream is = new FileInputStream(f);
		try(ArchiverValuesHandler handler = new ArchiverValuesHandler("DummyPVName", is, f.toString(), null)) { 
			HashMap<String, String> metaInfo = handler.getMetaInformation();
			for(String key : handler.getMetaInformation().keySet()) {
				logger.debug(key + "=" + handler.getMetaInformation().get(key));
			}
			Assertions.assertTrue((metaInfo.get("disp_low") != null), "Could not determine " + "disp_low");
			Assertions.assertTrue((metaInfo.get("disp_high") != null), "Could not determine " + "disp_high");
			Assertions.assertTrue((metaInfo.get("alarm_low") != null), "Could not determine " + "alarm_low");
			Assertions.assertTrue((metaInfo.get("alarm_high") != null), "Could not determine " + "alarm_high");
			Assertions.assertTrue((metaInfo.get("warn_high") != null), "Could not determine " + "warn_high");
			Assertions.assertTrue((metaInfo.get("warn_low") != null), "Could not determine " + "warn_low");
			Assertions.assertTrue((metaInfo.get("units") != null), "Could not determine " + "units");
			Assertions.assertTrue((metaInfo.get("prec") != null), "Could not determine " + "prec");

			Assertions.assertTrue(handler.getValueType().getDBRType(handler.getElementCount()) == ArchDBRTypes.DBR_SCALAR_DOUBLE, "Type is " + handler.getValueType().getDBRType(handler.getElementCount()) + " expecting DBR_DOUBLE");
			Assertions.assertTrue(handler.getElementCount() == 1, "Element Count is " + handler.getElementCount());

			int expectedEventCount = 2;
			int eventCount = 0;
			for(Event event : handler) {
				StringBuilder eventStr = new StringBuilder();
				eventStr.append(event.getEpochSeconds() + "," + event.getSampleValue().toString());
				logger.debug(eventStr.toString());
				eventCount++;
			}

			Assertions.assertTrue(eventCount == expectedEventCount, "Expected " + expectedEventCount + " and got " + eventCount);
		}
	}



}
