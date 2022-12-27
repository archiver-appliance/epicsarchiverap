/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.LocalEpicsTests;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import junit.framework.TestCase;
/**
 * test of destroying channels
 * @author Luofeng Li
 *
 */
@Category(LocalEpicsTests.class)
public class ChannelDestoryTest extends TestCase {
	private static Logger logger = Logger.getLogger(ChannelDestoryTest.class.getName());
	private SIOCSetup ioc = null;
	private ConfigServiceForTests testConfigService;
	private WriterTest writer = new WriterTest();

	@Before
	public void setUp() throws Exception {
		ioc = new SIOCSetup();
		ioc.startSIOCWithDefaultDB();
		testConfigService = new ConfigServiceForTests(new File("./bin"));
		Thread.sleep(3000);
	}

	@After
	public void tearDown() throws Exception {

		testConfigService.shutdownNow();
		ioc.stopSIOC();

	}

	@Test
	public void testAll() {
	    scanChannelDestroy();
		monitorChannelDestroy();
	}
/**
 * test of destroying the channel of the pv in scan mode
 */
	private void scanChannelDestroy() {
		String pvName = "test_0";
		try {
			ArchiveEngine.archivePV(pvName, 2, SamplingMethod.SCAN, 60, writer,
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false, false);
			Thread.sleep(2000);

			ArchiveEngine.destoryPv(pvName, testConfigService);
			Thread.sleep(2000);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
			assertTrue("the channel for " + pvName
					+ " should be destroyed but it is not",
					archiveChannel == null);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}
/**
 * the test of destroying the channel of the pv in monitor mode
 */
	private void monitorChannelDestroy() {
		String pvName = "test_1";
		try {

			ArchiveEngine.archivePV(pvName, 0.1F, SamplingMethod.MONITOR, 60,
					writer, testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE,
					null, false, false);
			Thread.sleep(2000);

			ArchiveEngine.destoryPv(pvName, testConfigService);
			Thread.sleep(2000);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
			assertTrue("the channel for " + pvName
					+ " should be destroyed but it is not",
					archiveChannel == null);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}

}
