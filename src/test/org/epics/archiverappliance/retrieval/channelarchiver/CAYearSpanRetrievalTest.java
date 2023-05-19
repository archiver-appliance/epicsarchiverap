/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.channelarchiver;

import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.IntegrationTests;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.client.RawDataRetrievalAsEventStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test retrieval from a Channel Archiver XML file that spans multiple years.
 * @author mshankar
 *
 */
@Category(IntegrationTests.class)
public class CAYearSpanRetrievalTest {
	private static Logger logger = LogManager.getLogger(CAYearSpanTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();

	@Before
	public void setUp() throws Exception {
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
	}

	@After
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
	}

	@Test
	public void testYearSpanThruRetrieval() throws Exception {
		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "CAYearSpan";

		int expectedEventCount = 100;
		int eventCount = 0;
		int[] yearCount = new int[TimeUtils.getCurrentYear()+1000];

		// Now we try to retrieve this over the wire.
		RawDataRetrievalAsEventStream dataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		Timestamp startTime = TimeUtils.convertFromISO8601String("2010-11-24T00:00:00.000Z");
		Timestamp endTime = TimeUtils.convertFromISO8601String("2011-01-15T00:00:00.000Z");
		try(EventStream st = dataRetrieval.getDataForPVS(new String[] {pvName} , startTime, endTime, null)) {
			for(Event event : st) {
				StringBuilder eventStr = new StringBuilder();
				eventStr.append(TimeUtils.convertToHumanReadableString(event.getEpochSeconds()) + "," + event.getSampleValue().toString());
				logger.debug(eventStr.toString());
				short year = TimeUtils.computeYearForEpochSeconds(event.getEpochSeconds());
				yearCount[year]++;
				eventCount++;
			}
		}
		assertTrue("Expected " + expectedEventCount + " and got " + eventCount, eventCount == expectedEventCount);
		
		assertTrue("Expected 66 events in 2010; got " + yearCount[2010], yearCount[2010] == 66);
		assertTrue("Expected 34 events in 2011; got " + yearCount[2011], yearCount[2011] == 34);
		for(int i = 0; i < yearCount.length; i++) {
			if(i != 2010 && i != 2011) {
				
			}
		}
		
	}
}
