/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;


/**
 * test of destroying channels
 * @author Luofeng Li
 *
 */
@Tag("localEpics")
public class ChannelDestroyTest {
	private static Logger logger = LogManager.getLogger(ChannelDestroyTest.class.getName());
	private SIOCSetup ioc = null;
	private ConfigServiceForTests testConfigService;
	private FakeWriter writer = new FakeWriter();

	@BeforeEach
	public void setUp() throws Exception {
		ioc = new SIOCSetup();
		ioc.startSIOCWithDefaultDB();
		testConfigService = new ConfigServiceForTests(new File("./bin"));
		Thread.sleep(3000);
	}

	@AfterEach
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
			Assertions.assertTrue(archiveChannel == null, "the channel for " + pvName
					+ " should be destroyed but it is not");

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
			Assertions.assertTrue(archiveChannel == null, "the channel for " + pvName
					+ " should be destroyed but it is not");

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}

}
