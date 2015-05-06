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
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.retrieval.ChangeInYearsException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test parsing a Channel Archiver XML file that spans multiple years. 
 * @author mshankar
 *
 */
public class CAYearSpanTest {
	private static Logger logger = Logger.getLogger(CAYearSpanTest.class.getName());

	@Before
	public void setUp() throws Exception {
	}

	@After
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

			assertTrue("Expected  an exception to be thrown", expectionThrown);

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

			assertTrue("Expected exception on change in year" + yearOfPreviousEvent + "/" + yearOfFirstEventAfterException, yearOfFirstEventAfterException > yearOfPreviousEvent);

			assertTrue("Expected " + expectedEventCount + " and got " + eventCount, eventCount == expectedEventCount);
			assertTrue("Expected 66 events in 2010; got " + yearCount[2010], yearCount[2010] == 66);
			assertTrue("Expected 34 events in 2011; got " + yearCount[2011], yearCount[2011] == 34);
		}
	}
}
