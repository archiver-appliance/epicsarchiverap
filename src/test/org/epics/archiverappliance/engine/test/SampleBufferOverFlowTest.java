/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * test of sample buffer over flow
 * @author Luofeng Li
 *
 */
@Tag("localEpics")
public class SampleBufferOverFlowTest {
	private static final Logger logger = LogManager.getLogger(SampleBufferOverFlowTest.class.getName());
	private SIOCSetup ioc = null;
	private ConfigServiceForTests testConfigService;
	private FakeWriter writer = new FakeWriter();

    private final String pvPrefix = SampleBufferOverFlowTest.class.getSimpleName().substring(0, 10);

    @BeforeEach
	public void setUp() throws Exception {
		ioc = new SIOCSetup(pvPrefix);
		ioc.startSIOCWithDefaultDB();
        testConfigService = new ConfigServiceForTests(-1);
		Thread.sleep(3000);
	}

	@AfterEach
	public void tearDown() throws Exception {
		testConfigService.shutdownNow();
		ioc.stopSIOC();
	}

	@Test
	public void sampleBufferOverflow() {
		String pvName = pvPrefix + "test_1000";
		try {
			ArchiveEngine.archivePV(pvName, 5F, SamplingMethod.MONITOR,
					writer, testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE,
					null, false, false);

			Thread.sleep(30000);

			long num = ArchiveEngine.getMetricsforPV(pvName, testConfigService)
					.getSampleBufferFullLostEventCount();
			Assertions.assertTrue(num > 0, "the number of data lost because of sample buffer overflow of "
					+ pvName
					+ "is 0,and maybe "
					+ "the pv of "
					+ pvName
					+ " changes too slow and for this test,it should changes every 1 second");

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}

}
