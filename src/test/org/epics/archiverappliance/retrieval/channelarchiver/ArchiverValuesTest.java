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

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Simple unit test using a sample from LCLS production for DBR_SCALAR_DOUBLE.
 * @author mshankar
 *
 */
public class ArchiverValuesTest {
	private static Logger logger = Logger.getLogger(ArchiverValuesTest.class.getName());

	@Before
	public void setUp() throws Exception {
	}

	@After
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
			assertTrue("Could not determine " + "disp_low", (metaInfo.get("disp_low") != null));
			assertTrue("Could not determine " + "disp_high", (metaInfo.get("disp_high") != null));
			assertTrue("Could not determine " + "alarm_low", (metaInfo.get("alarm_low") != null));
			assertTrue("Could not determine " + "alarm_high", (metaInfo.get("alarm_high") != null));
			assertTrue("Could not determine " + "warn_high", (metaInfo.get("warn_high") != null));
			assertTrue("Could not determine " + "warn_low", (metaInfo.get("warn_low") != null));
			assertTrue("Could not determine " + "units", (metaInfo.get("units") != null));
			assertTrue("Could not determine " + "prec", (metaInfo.get("prec") != null));

			assertTrue("Type is " + handler.getValueType().getDBRType(handler.getElementCount()) + " expecting DBR_DOUBLE", handler.getValueType().getDBRType(handler.getElementCount()) == ArchDBRTypes.DBR_SCALAR_DOUBLE);
			assertTrue("Element Count is " + handler.getElementCount(), handler.getElementCount() == 1);

			int expectedEventCount = 100;
			int eventCount = 0;
			for(Event event : handler) {
				StringBuilder eventStr = new StringBuilder();
				eventStr.append(event.getEpochSeconds() + "," + event.getSampleValue().toString());
				logger.debug(eventStr.toString());
				eventCount++;
			}

			assertTrue("Expected " + expectedEventCount + " and got " + eventCount, eventCount == expectedEventCount);
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
			assertTrue("Could not determine " + "disp_low", (metaInfo.get("disp_low") != null));
			assertTrue("Could not determine " + "disp_high", (metaInfo.get("disp_high") != null));
			assertTrue("Could not determine " + "alarm_low", (metaInfo.get("alarm_low") != null));
			assertTrue("Could not determine " + "alarm_high", (metaInfo.get("alarm_high") != null));
			assertTrue("Could not determine " + "warn_high", (metaInfo.get("warn_high") != null));
			assertTrue("Could not determine " + "warn_low", (metaInfo.get("warn_low") != null));
			assertTrue("Could not determine " + "units", (metaInfo.get("units") != null));
			assertTrue("Could not determine " + "prec", (metaInfo.get("prec") != null));

			assertTrue("Type is " + handler.getValueType().getDBRType(handler.getElementCount()) + " expecting DBR_WAVEFORM_DOUBLE", handler.getValueType().getDBRType(handler.getElementCount()) == ArchDBRTypes.DBR_WAVEFORM_DOUBLE);
			assertTrue("Element Count is " + handler.getElementCount(), handler.getElementCount() > 1);

			int expectedEventCount = 5;
			int eventCount = 0;
			for(Event event : handler) {
				StringBuilder eventStr = new StringBuilder();
				eventStr.append(event.getEpochSeconds() + "," + event.getSampleValue().toString());
				logger.debug(eventStr.toString());
				eventCount++;
			}

			assertTrue("Expected " + expectedEventCount + " and got " + eventCount, eventCount == expectedEventCount);
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
			assertTrue("Could not determine " + "disp_low", (metaInfo.get("disp_low") != null));
			assertTrue("Could not determine " + "disp_high", (metaInfo.get("disp_high") != null));
			assertTrue("Could not determine " + "alarm_low", (metaInfo.get("alarm_low") != null));
			assertTrue("Could not determine " + "alarm_high", (metaInfo.get("alarm_high") != null));
			assertTrue("Could not determine " + "warn_high", (metaInfo.get("warn_high") != null));
			assertTrue("Could not determine " + "warn_low", (metaInfo.get("warn_low") != null));
			assertTrue("Could not determine " + "units", (metaInfo.get("units") != null));
			assertTrue("Could not determine " + "prec", (metaInfo.get("prec") != null));

			assertTrue("Type is " + handler.getValueType().getDBRType(handler.getElementCount()) + " expecting DBR_SCALAR_ENUM", handler.getValueType().getDBRType(handler.getElementCount()) == ArchDBRTypes.DBR_SCALAR_ENUM);
			assertTrue("Element Count is " + handler.getElementCount(), handler.getElementCount() == 1);

			int expectedEventCount = 0;
			int eventCount = 0;
			for(Event event : handler) {
				StringBuilder eventStr = new StringBuilder();
				eventStr.append(event.getEpochSeconds() + "," + event.getSampleValue().toString());
				logger.debug(eventStr.toString());
				eventCount++;
			}

			assertTrue("Expected " + expectedEventCount + " and got " + eventCount, eventCount == expectedEventCount);
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
			assertTrue("Could not determine " + "disp_low", (metaInfo.get("disp_low") != null));
			assertTrue("Could not determine " + "disp_high", (metaInfo.get("disp_high") != null));
			assertTrue("Could not determine " + "alarm_low", (metaInfo.get("alarm_low") != null));
			assertTrue("Could not determine " + "alarm_high", (metaInfo.get("alarm_high") != null));
			assertTrue("Could not determine " + "warn_high", (metaInfo.get("warn_high") != null));
			assertTrue("Could not determine " + "warn_low", (metaInfo.get("warn_low") != null));
			assertTrue("Could not determine " + "units", (metaInfo.get("units") != null));
			assertTrue("Could not determine " + "prec", (metaInfo.get("prec") != null));

			assertTrue("Type is " + handler.getValueType().getDBRType(handler.getElementCount()) + " expecting DBR_DOUBLE", handler.getValueType().getDBRType(handler.getElementCount()) == ArchDBRTypes.DBR_SCALAR_DOUBLE);
			assertTrue("Element Count is " + handler.getElementCount(), handler.getElementCount() == 1);

			int expectedEventCount = 2;
			int eventCount = 0;
			for(Event event : handler) {
				StringBuilder eventStr = new StringBuilder();
				eventStr.append(event.getEpochSeconds() + "," + event.getSampleValue().toString());
				logger.debug(eventStr.toString());
				eventCount++;
			}

			assertTrue("Expected " + expectedEventCount + " and got " + eventCount, eventCount == expectedEventCount);
		}
	}



}
