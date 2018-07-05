/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.retrieval.client;


import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.EventStream;
import org.epics.archiverappliance.EventStreamDesc;
import org.epics.archiverappliance.TomcatSetup;
import org.epics.archiverappliance.common.TimeUtils;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.retrieval.GenerateData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Test retrieval for single PVs
 * @author mshankar
 *
 */
public class SinglePVRetrievalTest {
	private static final Logger logger = Logger.getLogger(SinglePVRetrievalTest.class.getName());
	TomcatSetup tomcatSetup = new TomcatSetup();
	
	@Before
	public void setUp() throws Exception {
		GenerateData.generateSineForPV(ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "Sine1", 0, ArchDBRTypes.DBR_SCALAR_DOUBLE);
		tomcatSetup.setUpWebApps(this.getClass().getSimpleName());
	}

	@After
	public void tearDown() throws Exception {
		tomcatSetup.tearDown();
	}

	@Test
	public void testGetDataForSinglePV() throws Exception {
		testGetOneDaysDataForYear(TimeUtils.getCurrentYear(), 86400);
		testGetOneDaysDataForYear(TimeUtils.getCurrentYear() - 1, 0);
		testGetOneDaysDataForYear(TimeUtils.getCurrentYear() + 1, 1);
	}
	
	private void testGetOneDaysDataForYear(int year, int expectedCount) throws Exception {
		RawDataRetrievalAsEventStream rawDataRetrieval = new RawDataRetrievalAsEventStream("http://localhost:" + ConfigServiceForTests.RETRIEVAL_TEST_PORT+ "/retrieval/data/getData.raw");
		Timestamp start = TimeUtils.convertFromISO8601String(year + "-02-01T08:00:00.000Z");
		Timestamp end = TimeUtils.convertFromISO8601String(year + "-02-02T08:00:00.000Z");
		
		
		EventStream stream = null;
		try {
			stream = rawDataRetrieval.getDataForPVS(new String[] { ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + "Sine1" }, start, end, new RetrievalEventProcessor() {
				@Override
				public void newPVOnStream(EventStreamDesc desc) {
					logger.info("Getting data for PV " + desc.getPvName());
				}
			});

			long previousEpochSeconds = 0;
			int eventCount = 0;

			// We are making sure that the stream we get back has times in sequential order...
			if(stream != null) {
				for(Event e : stream) {
					long actualSeconds = e.getEpochSeconds();
					assertTrue(actualSeconds >= previousEpochSeconds);
					previousEpochSeconds = actualSeconds;
					eventCount++;
				}
			}
			
			assertTrue("Event count is not what we expect. We got " + eventCount + " and we expected " + expectedCount + " for year " + year, eventCount == expectedCount);
		} finally {
			if(stream != null) try { stream.close(); stream = null; } catch(Throwable t) { }
		}
	}	
}
