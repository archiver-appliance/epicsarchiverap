/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.channelarchiver;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.retrieval.ChangeInYearsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test parsing a Channel Archiver XML file that spans multiple years. 
 * @author mshankar
 *
 */
public class CAYearSpanTest {
	private static Logger logger = LogManager.getLogger(CAYearSpanTest.class.getName());

	@BeforeEach
	public void setUp() throws Exception {
	}

	@AfterEach
	public void tearDown() throws Exception {
	}

	@Test
	public void testYearSpan() throws Exception {
		File f = new File("src/test/org/epics/archiverappliance/retrieval/channelarchiver/--ArchUnitTestCAYearSpan.xml");
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
			int[] yearCount = new int[TimeUtils.getCurrentYear()+1000];

			boolean expectionThrown = false;
			Event previousEvent = null;
			try {
				for(Event event : handler) {
					StringBuilder eventStr = new StringBuilder();
					eventStr.append(TimeUtils.convertToHumanReadableString(event.getEpochSeconds()) + "," + event.getSampleValue().toString());
					logger.debug(eventStr.toString());
					eventCount++;
					short year = TimeUtils.computeYearForEpochSeconds(event.getEpochSeconds());
					yearCount[year]++;
					previousEvent = event;
				}
			} catch(ChangeInYearsException ex) {
				logger.debug("Got the change in years exception");
				expectionThrown = true;
			}

			Assertions.assertTrue(expectionThrown, "Expected  an exception to be thrown");

			Event firstEventAfterException = null;
			for(Event event : handler) {
				if(firstEventAfterException == null) firstEventAfterException = event;
				StringBuilder eventStr = new StringBuilder();
				eventStr.append(TimeUtils.convertToHumanReadableString(event.getEpochSeconds()) + "," + event.getSampleValue().toString());
				logger.debug(eventStr.toString());
				short year = TimeUtils.computeYearForEpochSeconds(event.getEpochSeconds());
				yearCount[year]++;
				eventCount++;
			}

			short yearOfPreviousEvent = TimeUtils.computeYearForEpochSeconds(previousEvent.getEpochSeconds());
			short yearOfFirstEventAfterException = TimeUtils.computeYearForEpochSeconds(firstEventAfterException.getEpochSeconds());

			Assertions.assertTrue(yearOfFirstEventAfterException > yearOfPreviousEvent, "Expected exception on change in year" + yearOfPreviousEvent + "/" + yearOfFirstEventAfterException);

			Assertions.assertTrue(eventCount == expectedEventCount, "Expected " + expectedEventCount + " and got " + eventCount);
			Assertions.assertTrue(yearCount[2010] == 66, "Expected 66 events in 2010; got " + yearCount[2010]);
			Assertions.assertTrue(yearCount[2011] == 34, "Expected 34 events in 2011; got " + yearCount[2011]);
		}
	}
}
