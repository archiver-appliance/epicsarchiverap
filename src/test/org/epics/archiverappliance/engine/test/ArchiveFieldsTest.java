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
import org.epics.archiverappliance.Event;
import org.epics.archiverappliance.SIOCSetup;
import org.epics.archiverappliance.config.ArchDBRTypes;
import org.epics.archiverappliance.config.ConfigServiceForTests;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.data.DBRTimeEvent;
import org.epics.archiverappliance.engine.ArchiveEngine;
import org.epics.archiverappliance.engine.model.ArchiveChannel;
import org.epics.archiverappliance.mgmt.policy.PolicyConfig.SamplingMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * test for meta data archiving
 * 
 * @author Luofeng Li
 * 
 */
public class ArchiveFieldsTest extends TestCase {
	private static Logger logger = Logger.getLogger(ArchiveFieldsTest.class.getName());
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
		testConfigService.getEngineContext().setDisconnectCheckTimeoutInMinutesForTestingPurposesOnly(1);
		Thread.sleep(3000);
	}

	@After
	public void tearDown() throws Exception {

		testConfigService.shutdownNow();
		ioc.stopSIOC();

	}

	@Test
	public void testAll() {
		OneChannelWithMetaField();
		OneChannelWithMetaFieldWithControlPv();
	}

	/**
	 * test one pv with meta field. We must make sure the meta fields should be
	 * archived too
	 */
	private void OneChannelWithMetaField() {

		try {
			String pvName = "test_NOADEL";
			MemBufWriter myWriter = new MemBufWriter(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE);
			PVTypeInfo typeInfo = new PVTypeInfo(pvName, ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			typeInfo.addArchiveField("HIHI");
			typeInfo.addArchiveField("LOLO");
			testConfigService.updateTypeInfoForPV(pvName, typeInfo);
			testConfigService.registerPVToAppliance(pvName, testConfigService.getMyApplianceInfo());

			ArchiveEngine.archivePV(pvName, 1, SamplingMethod.MONITOR, 60, myWriter, testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null, typeInfo.getArchiveFields(), false, false);
			Thread.sleep(15*1000);
			testConfigService.getEngineContext().getChannelList().get(pvName).startUpMetaChannels();
			Thread.sleep(15*1000);			
			assertTrue("Not emough delay - metafields still need starting up", !testConfigService.getEngineContext().getChannelList().get(pvName).metaChannelsNeedStartingUp());
			logger.info("Changing fields");
			SIOCSetup.caput(pvName + ".HIHI", 80);
			SIOCSetup.caput(pvName + ".LOLO", 5);
			Thread.sleep(1000);
			SIOCSetup.caput(pvName + ".HIHI", 85);
			SIOCSetup.caput(pvName + ".LOLO", 6);
			logger.info("Done changing fields");
			Thread.sleep(30000);

			int hihiNum = 0;
			int loloNUm = 0;
			int totalEvents = 0;
			for (Event e : myWriter.getCollectedSamples()) {
				DBRTimeEvent tempDBRTimeEvent = (DBRTimeEvent) e;
				String hihiVluue = tempDBRTimeEvent.getFieldValue("HIHI");
				if (hihiVluue != null) {
					hihiNum++;
				}

				String loloVluue = tempDBRTimeEvent.getFieldValue("LOLO");
				if (loloVluue != null) {
					loloNUm++;
				}
				totalEvents++;
			}
			assertTrue("We should have some events in the current samples " + totalEvents, totalEvents >= 2);
			assertTrue("the number of value for test_0.HIHI num is " + hihiNum
					+ " and <3 and" + "it should be >=3", hihiNum >= 2);
			assertTrue("the number of value for test_0.LOLO num is " + loloNUm
					+ " and <3 and" + "it should be >=3", loloNUm >= 2);
			Thread.sleep(3000);

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}

	/**
	 * test one pv with meta field.this pv and the meta fields are controlled by
	 * another pv to start or stop archiving. We must make sure when the pv is
	 * stopped or started archiving ,all the meta field should be stopped or
	 * stated at the same time
	 */
	private void OneChannelWithMetaFieldWithControlPv() {

		try {
			String controlPVName = "test:enable0";
			SIOCSetup.caput(controlPVName, 1);
			Thread.sleep(3000);
			String[] metaFields = { "HIHI", "LOLO" };
			String pvName = "test_1";
			PVTypeInfo typeInfo = new PVTypeInfo(pvName,ArchDBRTypes.DBR_SCALAR_DOUBLE, true, 1);
			typeInfo.setSamplingMethod(SamplingMethod.SCAN);
			typeInfo.setSamplingPeriod(60);
			typeInfo.setDataStores(new String[] {"blackhole://localhost"});
			typeInfo.setArchiveFields(metaFields);
			typeInfo.setControllingPV(controlPVName);
			testConfigService.updateTypeInfoForPV(pvName, typeInfo);
			ArchiveEngine.archivePV(pvName, 2, SamplingMethod.SCAN, 60, writer,
					testConfigService, ArchDBRTypes.DBR_SCALAR_DOUBLE, null,
					controlPVName, metaFields, null, false, false);
			Thread.sleep(15*1000);
			testConfigService.getEngineContext().getChannelList().get(pvName).startUpMetaChannels();
			Thread.sleep(15*1000);			
			assertTrue("Not emough delay - metafields still need starting up", !testConfigService.getEngineContext().getChannelList().get(pvName).metaChannelsNeedStartingUp());
			ArchiveChannel archiveChannel = testConfigService
					.getEngineContext().getChannelList().get(pvName);

			boolean result = archiveChannel.isConnected()
					&& archiveChannel.getSampleBuffer().getCurrentSamples()
							.size() > 0;
			assertTrue(pvName + "is not started successfully and it shoule be started successfully", result);

			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				assertTrue("the channel for " + pvNameTemp + " should be created and connected but it is not", archiveChannel.isMetaPVConnected(metaFieldTemp));

			}
			Thread.sleep(10000);
			SIOCSetup.caput(controlPVName, 0);
			testConfigService.getEngineContext().getWriteThead().flushBuffer();
			Thread.sleep(10000);
			archiveChannel = testConfigService.getEngineContext().getChannelList().get(pvName);
			assertTrue(pvName + "is not stopped successfully and it should be stopped successfully", archiveChannel == null || !archiveChannel.isConnected());
			assertTrue(pvName + "should not have any data", archiveChannel == null || archiveChannel.getSampleBuffer().getCurrentSamples().size() == 0);

			if(archiveChannel != null) { 
				for (String metaFieldTemp : metaFields) {
					String pvNameTemp = pvName + "." + metaFieldTemp;
					assertTrue("the channel for " + pvNameTemp
							+ " should be not connected but it is ",
							!archiveChannel.isMetaPVConnected(metaFieldTemp));
				}
			}

			Thread.sleep(10000);
			SIOCSetup.caput(controlPVName, 1);
			Thread.sleep(30000);
			archiveChannel = testConfigService.getEngineContext().getChannelList().get(pvName);
			assertTrue("After resuming the control channel, the archive channel for pv " + pvName + " is still null", archiveChannel != null);
			boolean result3 = archiveChannel.isConnected();
			assertTrue(
					pvName
							+ "is not started successfully and it should be started successfully",
					result3);
			
			Thread.sleep(15*1000);
			testConfigService.getEngineContext().getChannelList().get(pvName).startUpMetaChannels();
			Thread.sleep(15*1000);			
			assertTrue("Not emough delay - metafields still need starting up", !testConfigService.getEngineContext().getChannelList().get(pvName).metaChannelsNeedStartingUp());

			// check meta field is not connected
			for (String metaFieldTemp : metaFields) {
				String pvNameTemp = pvName + "." + metaFieldTemp;
				assertTrue("the channel for " + pvNameTemp
						+ " should be reconnected but it is  not",
						archiveChannel.isMetaPVConnected(metaFieldTemp));

			}

		} catch (Exception e) {
			//
			logger.error("Exception", e);
		}
	}

}