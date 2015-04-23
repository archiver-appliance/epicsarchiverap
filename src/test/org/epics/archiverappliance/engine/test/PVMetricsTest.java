/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.engine.test;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
/**
 * test of getting pv metrics
 * @author Luofeng Li
 *
 */
public class PVMetricsTest extends TestCase {
	private static Logger logger = Logger.getLogger(PVMetricsTest.class.getName());
	private SIOCSetup ioc = null;
	private ScheduledThreadPoolExecutor scheduler;
	private ConfigServiceForTests testConfigService;
	private WriterTest writer = new WriterTest();
	

	@Before
	public void setUp() throws Exception {
		ioc = new SIOCSetup();
		ioc.startSIOCWithDefaultDB();
		scheduler = (ScheduledThreadPoolExecutor) Executors
				.newScheduledThreadPool(1);
		testConfigService = new ConfigServiceForTests(new File("./bin"));
		testConfigService.getEngineContext().setScheduler(scheduler);
		Thread.sleep(3000);
	}

	@After
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
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null, false);
			Thread.sleep(2000);
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			// System.out.println(tempPVMetrics.getDetailedStatus());
			assertTrue("PVMetrics for " + pvName + " should not be null",
					tempPVMetrics != null);
			assertTrue(pvName + " should not be connected",
					tempPVMetrics.isConnected());
			assertTrue(pvName + " should  be in scan mode",
					!tempPVMetrics.isMonitor());

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
					null, false);
			Thread.sleep(2000);
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			// System.out.println(tempPVMetrics.getDetailedStatus());
			assertTrue("PVMetrics for " + pvName + " should not be null",
					tempPVMetrics != null);
			assertTrue(pvName + " should not be connected",
					tempPVMetrics.isConnected());
			assertTrue(pvName + " should  be in monitor mode",
					tempPVMetrics.isMonitor());

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}

}
