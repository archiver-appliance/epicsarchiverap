

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
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.membuf.ArrayListEventStream;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.engine.pv.PVMetrics;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
/**
 * test of creating channels
 * @author Luofeng Li
 *
 */
public class ChannelTest extends TestCase {
	private static Logger logger = Logger.getLogger(ChannelTest.class.getName());
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
		singleScanChannel();
		singleMonitorChannel();
		stopAndRestartChannel();
		create1000ScanChannel();
		create1000MonitorChannel();
		getPVdata();
	}
/**
 * test of creating the channel for the pv in scan mode
 */
	private void singleScanChannel() {

		String pvName = "test_0";
		try {
			ArchiveEngine.archivePV(pvName, 1, SamplingMethod.SCAN, 60, writer,
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null);

			Thread.sleep(62000);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
			assertTrue("the channel for " + pvName
					+ " should be created but it is not",
					archiveChannel != null);
			boolean hasData = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size() > 0;
			assertTrue("the channel for " + pvName
					+ " should have data but it don't", hasData);
			
			ArchiveEngine.destoryPv(pvName, testConfigService);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}
/**
 * test of creating the channel for the pv in monitor mode
 */
	private void singleMonitorChannel() {
		String pvName = "test_1";
		try {
			ArchiveEngine.archivePV(pvName, 0.1F, SamplingMethod.MONITOR, 60,
					writer, testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE,
					null);
			Thread.sleep(5000);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
			assertTrue("the channel for " + pvName
					+ " should be created but it is not",
					archiveChannel != null);
			boolean hasData = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size() > 0;
			assertTrue("the channel for " + pvName
					+ " should have data but it don't", hasData);

			ArchiveEngine.destoryPv(pvName, testConfigService);
		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}
/**
 * test of starting or stopping archiving one pv
 */
	private void stopAndRestartChannel() {

		String pvName = ConfigServiceForTests.ARCH_UNIT_TEST_PVNAME_PREFIX + ":test_2";
		try {

			PVTypeInfo typeInfo = new PVTypeInfo(pvName,ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			typeInfo.setSamplingMethod(SamplingMethod.SCAN);
			typeInfo.setSamplingPeriod(60);
			typeInfo.setDataStores(new String[] {"blackhole://localhost"});
			testConfigService.updateTypeInfoForPV(pvName, typeInfo);

			ArchiveEngine.archivePV(pvName, 1, SamplingMethod.SCAN, 60, writer,
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null);
			Thread.sleep(2000);
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);
			assertTrue("the channel for " + pvName
					+ " should be created but it is not",
					archiveChannel != null);
			boolean hasData = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size() > 0;
			assertTrue("the channel for " + pvName
					+ " should have data but it don't", hasData);
			ArchiveEngine.pauseArchivingPV(pvName, testConfigService);
			Thread.sleep(2000);
			archiveChannel.getSampleBuffer().getCurrentSamples().clear();
			Thread.sleep(2000);
			PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName, testConfigService);
			assertTrue("the channel for " + pvName
					+ " should be stopped but it is not",
					tempPVMetrics == null || !tempPVMetrics.isConnected());
			boolean hasData2 = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size() > 0;
			assertTrue("the channel for " + pvName
					+ " should not have data but it has", !hasData2);

			ArchiveEngine.resumeArchivingPV(pvName, testConfigService);
			Thread.sleep(62000);
			PVMetrics tempPVMetrics3 = ArchiveEngine.getMetricsforPV(pvName,
					testConfigService);
			assertTrue("the channel for " + pvName
					+ " should be restarted but it is not",
					tempPVMetrics3.isConnected());
			archiveChannel = testConfigService.getEngineContext().getChannelList().get(pvName);
			boolean hasData3 = archiveChannel.getSampleBuffer()
					.getCurrentSamples().size() > 0;
			assertTrue("the channel for " + pvName
					+ " should have data but it don't", hasData3);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}
/**
 * test of creating channels for 1000 pvs in scan mode 
 */
	private void create1000ScanChannel() {
		try {

			for (int m = 0; m < 1000; m++) {
				String pvName = "test_" + m;
				ArchiveEngine.archivePV(pvName, 0.1F, SamplingMethod.SCAN, 60,
						writer, testConfigService,
						ArchDBRTypes.DBR_SCALAR_DOUBLE, null);
				Thread.sleep(10);
			}
			Thread.sleep(2000);
			int num = 0;
			for (int m = 0; m < 1000; m++) {
				String pvName = "test_" + m;
				PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
						testConfigService);
				if (tempPVMetrics.isConnected())
					num++;

			}
			assertTrue(
					"Only "
							+ num
							+ " of 1000 of channels in scan mode connected successfully",
					num == 1000);
			for (int m = 0; m < 1000; m++) {
				String pvName = "test_" + m;
				ArchiveEngine.destoryPv(pvName, testConfigService);
			}

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}
/**
 * test of creating channels for 1000 pvs in monitor mode
 */
	private void create1000MonitorChannel() {
		try {

			for (int m = 1000; m < 2000; m++) {
				String pvName = "test_" + m;
				ArchiveEngine.archivePV(pvName, 0.1F, SamplingMethod.MONITOR,
						60, writer, testConfigService,
						ArchDBRTypes.DBR_SCALAR_DOUBLE, null);
				Thread.sleep(10);
			}

			Thread.sleep(2000);
			int num = 0;
			for (int m = 1000; m < 2000; m++) {
				String pvName = "test_" + m;
				PVMetrics tempPVMetrics = ArchiveEngine.getMetricsforPV(pvName,
						testConfigService);
				if (tempPVMetrics.isConnected())
					num++;

			}
			assertTrue(
					"Only "
							+ num
							+ " of 1000 of channels in monitor mode  connected successfully",
					num == 1000);

			for (int m = 0; m < 1000; m++) {
				String pvName = "test_" + m;
				ArchiveEngine.destoryPv(pvName, testConfigService);
			}
		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}
/**
 * test of getting pv combined data of previous and current ArrayListEventStream
 */
	private void getPVdata() {
		String pvName = "test_2";
		try {

			ArchiveEngine.archivePV(pvName, 2, SamplingMethod.SCAN, 60, writer,
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null);
			Thread.sleep(2000);
			ArrayListEventStream samples = testConfigService.getEngineContext().getChannelList().get(pvName).getPVData();

			assertTrue("there is no data in sample buffer", samples.size() > 0);
			ArchiveEngine.destoryPv(pvName, testConfigService);
		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}

	}
}
