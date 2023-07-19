/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;


/**
 * test of engine shuting down
 * @author Luofeng Li
 *
 */
@Tag("localEpics")
public class EngineShutDownTest {
	private static Logger logger = LogManager.getLogger(EngineShutDownTest.class.getName());
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

	
		ioc.stopSIOC();

	}

	@Test
	public void testAll() {
		engineShutDown();
	}
/**
 * test of engine shutting down
 */
	private void engineShutDown()

	{

		try {
			for (int m = 0; m < 100; m++) {
				ArchiveEngine.archivePV("test_" + m, 0.1F, SamplingMethod.SCAN,
						5, writer, testConfigService,
						ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false, false);
				Thread.sleep(10);
			}
			Thread.sleep(2000);

			testConfigService.shutdownNow();
			Thread.sleep(2000);
			int num = 0;
			Iterator<String> allpvs = testConfigService
					.getPVsForThisAppliance().iterator();
			while (allpvs.hasNext()) {
				allpvs.next();
				num++;
			}
			

			Assertions.assertTrue(num == 0, "there should be no pvs after the engine shut down, but there are "
					+ num + " pvs");

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}

}
