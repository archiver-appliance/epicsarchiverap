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
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;


import org.junit.jupiter.api.Test;

/**
 * test of getting pv metrics
 * @author Luofeng Li
 *
 */
@Tag("localEpics")
public class PVMetricsTest {
	private static Logger logger = LogManager.getLogger(PVMetricsTest.class.getName());
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
		PVMetricsForSingleScanChannel();
		PVMetricsForSingleMonitorChannel();
	}
/**
 * test of getting pv metrics for one pv in scan mode
 */
	private void PVMetricsForSingleScanChannel() {
		String pvName = "test_0";
		try {

			ArchiveEngine.archivePV(pvName, 2, SamplingMethod.SCAN, 5, writer,
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false, false);
			Thread.sleep(2000);
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			// System.out.println(tempPVMetrics.getDetailedStatus());
			Assertions.assertTrue(tempPVMetrics != null, "PVMetrics for " + pvName + " should not be null");
			Assertions.assertTrue(tempPVMetrics.isConnected(), pvName + " should not be connected");
			Assertions.assertTrue(!tempPVMetrics.isMonitor(), pvName + " should  be in scan mode");

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}
/**
 * test of getting pv metrics of one pv in monitor mode
 */
	private void PVMetricsForSingleMonitorChannel() {
		String pvName = "test_1";
		try {

			ArchiveEngine.archivePV(pvName, 2, SamplingMethod.MONITOR, 5,
					writer, testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE,
					null, false, false);
			Thread.sleep(2000);
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			// System.out.println(tempPVMetrics.getDetailedStatus());
			Assertions.assertTrue(tempPVMetrics != null, "PVMetrics for " + pvName + " should not be null");
			Assertions.assertTrue(tempPVMetrics.isConnected(), pvName + " should not be connected");
			Assertions.assertTrue(tempPVMetrics.isMonitor(), pvName + " should  be in monitor mode");

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}

}
